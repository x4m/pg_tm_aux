/* contrib/pg_tm_aux/pg_tm_aux--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pg_tm_aux" to load this file. \quit

--
-- pg_create_logical_replication_slot_lsn()
--
CREATE OR REPLACE FUNCTION pg_create_logical_replication_slot_lsn(
    IN slot_name name, IN plugin name,
    IN temporary boolean DEFAULT false, IN restart_lsn pg_lsn DEFAULT null,
    OUT slot_name name, OUT lsn pg_lsn)
RETURNS RECORD
LANGUAGE C
STRICT VOLATILE 
AS 'MODULE_PATHNAME', 'pg_create_logical_replication_slot_lsn';

-- REVOKE ALL ON FUNCTION pg_create_logical_replication_slot_lsn(text, text, bool, pg_lsn) FROM PUBLIC;
