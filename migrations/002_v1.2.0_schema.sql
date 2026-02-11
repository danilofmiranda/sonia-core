-- Migration 002: SonIA Core v1.2.0 schema updates
-- Add tenant_name column to tenant_mapping (was client_name in v1.1.0)

ALTER TABLE tenant_mapping ADD COLUMN IF NOT EXISTS tenant_name VARCHAR(255);

-- Copy existing client_name values into tenant_name
UPDATE tenant_mapping SET tenant_name = client_name WHERE tenant_name IS NULL AND client_name IS NOT NULL;

-- Add unique constraint on client_contacts if not exists
DO $$ BEGIN
    IF NOT EXISTS (
          SELECT 1 FROM pg_constraint WHERE conname = 'uq_client_contact_name'
      ) THEN
        ALTER TABLE client_contacts ADD CONSTRAINT uq_client_contact_name UNIQUE (client_id, name);
    END IF;
END $$;
