-- Replace BIGSERIAL (auto-increment) with BIGINT for all analytics tables.
-- IDs will be generated as Snowflake IDs by the application.
-- Existing sentinel rows (id=1) remain untouched.

-- user_agents: drop default and sequence
ALTER TABLE user_agents ALTER COLUMN id DROP DEFAULT;
DROP SEQUENCE IF EXISTS user_agents_id_seq;

-- referers: drop default and sequence
ALTER TABLE referers ALTER COLUMN id DROP DEFAULT;
DROP SEQUENCE IF EXISTS referers_id_seq;

-- referer_domains: drop default and sequence
ALTER TABLE referer_domains ALTER COLUMN id DROP DEFAULT;
DROP SEQUENCE IF EXISTS referer_domains_id_seq;

-- geo_locations: drop default and sequence
ALTER TABLE geo_locations ALTER COLUMN id DROP DEFAULT;
DROP SEQUENCE IF EXISTS geo_locations_id_seq;

-- redirect_events: drop default and sequence
ALTER TABLE redirect_events ALTER COLUMN id DROP DEFAULT;
DROP SEQUENCE IF EXISTS redirect_events_id_seq;

-- Update comments
COMMENT ON COLUMN user_agents.id IS 'Snowflake ID primary key';
COMMENT ON COLUMN referers.id IS 'Snowflake ID primary key';
COMMENT ON COLUMN referer_domains.id IS 'Snowflake ID primary key';
COMMENT ON COLUMN geo_locations.id IS 'Snowflake ID primary key';
COMMENT ON COLUMN redirect_events.id IS 'Snowflake ID (part of composite PK)';
