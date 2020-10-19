/* contrib/pg_tm_aux/pg_tm_aux--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pg_tm_aux" to load this file. \quit

--
-- pg_create_logical_replication_slot_lsn()
--
CREATE FUNCTION pg_create_logical_replication_slot_lsn(text, text, bool, pg_lsn)
RETURNS RECORD
AS 'MODULE_PATHNAME', 'pg_create_logical_replication_slot_lsn'
LANGUAGE C STRICT VOLATILE RESTRICTED;

REVOKE ALL ON FUNCTION pg_create_logical_replication_slot_lsn(text, text, bool, pg_lsn) FROM PUBLIC;
