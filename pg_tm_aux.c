/*-------------------------------------------------------------------------
 *
 * pg_tm_aux.c
 *		Transfer manager auxilary functions
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/htup_details.h"
#include "access/timeline.h"
#include "access/xlog_internal.h"
#include "access/xlogutils.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "replication/decode.h"
#include "replication/logical.h"
#include "replication/slot.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/inval.h"
#include "utils/pg_lsn.h"
#include "utils/resowner.h"

#if (PG_VERSION_NUM < 130000)
#include "replication/logicalfuncs.h"
#endif


PG_MODULE_MAGIC;

static void
check_permissions(void)
{
	if (!superuser() && !has_rolreplication(GetUserId()))
	{
		Oid role = get_role_oid("mdb_replication", true);
		if (is_member_of_role(GetUserId(), role))
			return;
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("must be superuser or replication role to use replication slots")));
	}
}

static void check_lsn_not_on_current_timeline(XLogRecPtr target_lsn)
{
	List	   *timelineHistory = readTimeLineHistory(ThisTimeLineID);
	TimeLineID target_tli = tliOfPointInHistory(target_lsn, timelineHistory);
	list_free_deep(timelineHistory);

	if (target_tli == ThisTimeLineID)
		elog(ERROR, "This timeline %u includes slot LSN %X/%X. The slot must be created before switchover.",
				ThisTimeLineID,
				(uint32) (target_lsn >> 32),
				(uint32) (target_lsn));
}

// We have to hack CreateInitDecodingContext() when it was without restart_lsn
#if PG_VERSION_NUM < 120000

#include "utils/memutils.h"
#include "storage/procarray.h"
#include "access/xact.h"
/*
 * Create a new decoding context, for a new logical slot.
 *
 * plugin contains the name of the output plugin
 * output_plugin_options contains options passed to the output plugin
 * read_page, prepare_write, do_write, update_progress
 *		callbacks that have to be filled to perform the use-case dependent,
 *		actual, work.
 *
 * Needs to be called while in a memory context that's at least as long lived
 * as the decoding context because further memory contexts will be created
 * inside it.
 *
 * Returns an initialized decoding context after calling the output plugin's
 * startup function.
 */
static LogicalDecodingContext *
CreateInitDecodingContextExt(char *plugin,
						  List *output_plugin_options,
						  bool need_full_snapshot,
						  XLogPageReadCB read_page,
						  LogicalOutputPluginWriterPrepareWrite prepare_write,
						  LogicalOutputPluginWriterWrite do_write,
						  LogicalOutputPluginWriterUpdateProgress update_progress,
						  XLogRecPtr restart_lsn)
{
	TransactionId xmin_horizon = InvalidTransactionId;
	ReplicationSlot *slot;

	/* shorter lines... */
	slot = MyReplicationSlot;

	/* first some sanity checks that are unlikely to be violated */
	if (slot == NULL)
		elog(ERROR, "cannot perform logical decoding without an acquired slot");

	if (plugin == NULL)
		elog(ERROR, "cannot initialize logical decoding without a specified plugin");

	/* Make sure the passed slot is suitable. These are user facing errors. */
	if (SlotIsPhysical(slot))
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("cannot use physical replication slot for logical decoding")));

	if (slot->data.database != MyDatabaseId)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("replication slot \"%s\" was not created in this database",
						NameStr(slot->data.name))));

	if (IsTransactionState() &&
		GetTopTransactionIdIfAny() != InvalidTransactionId)
		ereport(ERROR,
				(errcode(ERRCODE_ACTIVE_SQL_TRANSACTION),
				 errmsg("cannot create logical replication slot in transaction that has performed writes")));

	/* register output plugin name with slot */
	SpinLockAcquire(&slot->mutex);
	StrNCpy(NameStr(slot->data.plugin), plugin, NAMEDATALEN);
	SpinLockRelease(&slot->mutex);

	if (XLogRecPtrIsInvalid(restart_lsn))
		ReplicationSlotReserveWal();
	else
	{
		SpinLockAcquire(&slot->mutex);
		slot->data.restart_lsn = restart_lsn;
		SpinLockRelease(&slot->mutex);
	}

	/* ----
	 * This is a bit tricky: We need to determine a safe xmin horizon to start
	 * decoding from, to avoid starting from a running xacts record referring
	 * to xids whose rows have been vacuumed or pruned
	 * already. GetOldestSafeDecodingTransactionId() returns such a value, but
	 * without further interlock its return value might immediately be out of
	 * date.
	 *
	 * So we have to acquire the ProcArrayLock to prevent computation of new
	 * xmin horizons by other backends, get the safe decoding xid, and inform
	 * the slot machinery about the new limit. Once that's done the
	 * ProcArrayLock can be released as the slot machinery now is
	 * protecting against vacuum.
	 *
	 * Note that, temporarily, the data, not just the catalog, xmin has to be
	 * reserved if a data snapshot is to be exported.  Otherwise the initial
	 * data snapshot created here is not guaranteed to be valid. After that
	 * the data xmin doesn't need to be managed anymore and the global xmin
	 * should be recomputed. As we are fine with losing the pegged data xmin
	 * after crash - no chance a snapshot would get exported anymore - we can
	 * get away with just setting the slot's
	 * effective_xmin. ReplicationSlotRelease will reset it again.
	 *
	 * ----
	 */
	LWLockAcquire(ProcArrayLock, LW_EXCLUSIVE);

	xmin_horizon = GetOldestSafeDecodingTransactionId(!need_full_snapshot);

	slot->effective_catalog_xmin = xmin_horizon;
	slot->data.catalog_xmin = xmin_horizon;
	if (need_full_snapshot)
		slot->effective_xmin = xmin_horizon;

	ReplicationSlotsComputeRequiredXmin(true);

	LWLockRelease(ProcArrayLock);

	ReplicationSlotMarkDirty();
	ReplicationSlotSave();

	return NULL;
}
#endif

/*
 * Helper function for creating a new logical replication slot with
 * given arguments. Note that this function doesn't release the created
 * slot.
 *
 * When find_startpoint is false, the slot's confirmed_flush is not set; it's
 * caller's responsibility to ensure it's set to something sensible.
 */
static void
create_logical_replication_slot(char *name, char *plugin,
								bool temporary, XLogRecPtr restart_lsn)
{
	LogicalDecodingContext *ctx = NULL;
	Assert(!MyReplicationSlot);

	/*
	 * Acquire a logical decoding slot, this will check for conflicting names.
	 * Initially create persistent slot as ephemeral - that allows us to
	 * nicely handle errors during initialization because it'll get dropped if
	 * this transaction fails. We'll make it persistent at the end. Temporary
	 * slots can be created as temporary from beginning as they get dropped on
	 * error as well.
	 */
#if (PG_VERSION_NUM >= 140000)
	ReplicationSlotCreate(name, true,
						  temporary ? RS_TEMPORARY : RS_EPHEMERAL, false);
#else
	ReplicationSlotCreate(name, true,
						  temporary ? RS_TEMPORARY : RS_EPHEMERAL);
#endif

	/* We intentionaly ignore values found by create_logical_replication_slot */
	/* This actually moves slot backwards and constitues race condition */
	SpinLockAcquire(&MyReplicationSlot->mutex);
	MyReplicationSlot->data.restart_lsn = restart_lsn;
	MyReplicationSlot->data.confirmed_flush = restart_lsn;
	SpinLockRelease(&MyReplicationSlot->mutex);


	/*
	 * Create logical decoding context to find start point or, if we don't
	 * need it, to 1) bump slot's restart_lsn and xmin 2) check plugin sanity.
	 *
	 * Note: when !find_startpoint this is still important, because it's at
	 * this point that the output plugin is validated.
	 */
#if (PG_VERSION_NUM >= 130000)
	ctx = CreateInitDecodingContext(plugin, NIL,
									false,	/* just catalogs is OK */
									restart_lsn,
									XL_ROUTINE(.page_read = read_local_xlog_page,
											   .segment_open = wal_segment_open,
											   .segment_close = wal_segment_close),
									NULL, NULL, NULL);
#elif (PG_VERSION_NUM >= 120000)
	ctx = CreateInitDecodingContext(plugin, NIL,
									false,	/* just catalogs is OK */
									restart_lsn,
									logical_read_local_xlog_page, NULL, NULL,
									NULL);
#else
	ctx = CreateInitDecodingContextExt(plugin, NIL,
									false,	/* do not build snapshot */
									logical_read_local_xlog_page, NULL, NULL,
									NULL, restart_lsn);
#endif

	/* don't need the decoding context anymore */
	if (ctx != NULL)
		FreeDecodingContext(ctx);
}

PG_FUNCTION_INFO_V1(pg_create_logical_replication_slot_lsn);

/*
 * SQL function for creating a new logical replication slot for a given LSN.
 */
Datum
pg_create_logical_replication_slot_lsn(PG_FUNCTION_ARGS)
{
	Name		name = PG_GETARG_NAME(0);
	Name		plugin = PG_GETARG_NAME(1);
	bool		temporary = PG_GETARG_BOOL(2);
	XLogRecPtr	restart_lsn = PG_GETARG_LSN(3);
	bool		force = false;
	Datum		result;
	TupleDesc	tupdesc;
	HeapTuple	tuple;
	Datum		values[2];
	bool		nulls[2];
	
	if (PG_NARGS() >= 5)
		force = PG_GETARG_BOOL(4);

	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	check_permissions();

	if (!force)
		check_lsn_not_on_current_timeline(restart_lsn);

	CheckLogicalDecodingRequirements();

	create_logical_replication_slot(NameStr(*name),
									NameStr(*plugin),
									temporary,
									restart_lsn);

	values[0] = NameGetDatum(&MyReplicationSlot->data.name);
	values[1] = LSNGetDatum(MyReplicationSlot->data.confirmed_flush);

	memset(nulls, 0, sizeof(nulls));

	tuple = heap_form_tuple(tupdesc, values, nulls);
	result = HeapTupleGetDatum(tuple);

	/* ok, slot is now fully created, mark it as persistent if needed */
	if (!temporary)
		ReplicationSlotPersist();
	ReplicationSlotRelease();

	PG_RETURN_DATUM(result);
}
