/*-------------------------------------------------------------------------
 *
 * pg_tm_aux.c
 *		Transfer manager auxilary functions
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/htup_details.h"
#include "access/xlog_internal.h"
#include "access/xlogutils.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "replication/decode.h"
#include "replication/logical.h"
#include "replication/slot.h"
#include "utils/builtins.h"
#include "utils/inval.h"
#include "utils/pg_lsn.h"
#include "utils/resowner.h"


PG_MODULE_MAGIC;

static void
check_permissions(void)
{
	if (!superuser() && !has_rolreplication(GetUserId()))
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("must be superuser or replication role to use replication slots")));
}

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
								bool temporary, XLogRecPtr restart_lsn,
								bool find_startpoint)
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
	ReplicationSlotCreate(name, true,
						  temporary ? RS_TEMPORARY : RS_EPHEMERAL);

	/*
	 * Create logical decoding context to find start point or, if we don't
	 * need it, to 1) bump slot's restart_lsn and xmin 2) check plugin sanity.
	 *
	 * Note: when !find_startpoint this is still important, because it's at
	 * this point that the output plugin is validated.
	 */
	ctx = CreateInitDecodingContext(plugin, NIL,
									false,	/* just catalogs is OK */
									restart_lsn,
									XL_ROUTINE(.page_read = read_local_xlog_page,
											   .segment_open = wal_segment_open,
											   .segment_close = wal_segment_close),
									NULL, NULL, NULL);

	/*
	 * If caller needs us to determine the decoding start point, do so now.
	 * This might take a while.
	 */
	if (find_startpoint)
		DecodingContextFindStartpoint(ctx);

	/* don't need the decoding context anymore */
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
	Datum		result;
	TupleDesc	tupdesc;
	HeapTuple	tuple;
	Datum		values[2];
	bool		nulls[2];

	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	check_permissions();

	CheckLogicalDecodingRequirements();

	create_logical_replication_slot(NameStr(*name),
									NameStr(*plugin),
									temporary,
									restart_lsn,
									true);

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