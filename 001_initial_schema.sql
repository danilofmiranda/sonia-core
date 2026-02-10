-- ============================================================================
-- SonIA Platform — PostgreSQL Schema
-- Migration 001: Initial Schema
-- BloomsPal / Fase 1
-- ============================================================================

-- Extensiones necesarias
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- ============================================================================
-- ENUM TYPES
-- ============================================================================

CREATE TYPE shipment_status AS ENUM (
    'label_created',
    'picked_up',
    'in_transit',
    'in_customs',
    'out_for_delivery',
    'delivered',
    'exception',
    'delayed',
    'on_hold',
    'delivery_attempted',
    'returned_to_sender',
    'cancelled',
    'unknown'
);

CREATE TYPE claim_type AS ENUM (
    'no_entregado',
    'danado',
    'perdida_total',
    'entrega_tardia',
    'direccion_incorrecta',
    'otro'
);

CREATE TYPE claim_status AS ENUM (
    'nuevo',
    'revision_interna',
    'enviado_fedex',
    'investigacion_fedex',
    'aprobado',
    'rechazado',
    'reembolso_recibido',
    'cerrado'
);

CREATE TYPE claim_origin AS ENUM (
    'manual',
    'proactivo_tracker',
    'whatsapp_agent'   -- Preparado para Fase 2
);

CREATE TYPE portal_user_role AS ENUM (
    'bloomspal_admin',
    'bloomspal_team',
    'fedex_exec'
);

CREATE TYPE run_status AS ENUM (
    'running',
    'success',
    'partial',
    'failed'
);

-- ============================================================================
-- TABLA: portal_users (usuarios del portal de reclamos)
-- NOTA: Va primero porque claims tiene FK a esta tabla
-- ============================================================================

CREATE TABLE portal_users (
    id              SERIAL PRIMARY KEY,
    username        VARCHAR(100) NOT NULL UNIQUE,
    email           VARCHAR(255) NOT NULL UNIQUE,
    password_hash   VARCHAR(255) NOT NULL,
    role            portal_user_role NOT NULL,
    full_name       VARCHAR(255) NOT NULL,
    is_active       BOOLEAN DEFAULT TRUE,
    last_login      TIMESTAMPTZ,
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);

-- ============================================================================
-- TABLA: clients (clientes/marcas — cacheados de Odoo)
-- ============================================================================

CREATE TABLE clients (
    id              SERIAL PRIMARY KEY,
    odoo_id         INTEGER UNIQUE,           -- ID en Odoo
    name            VARCHAR(255) NOT NULL,     -- Nombre de la marca/compañía
    dynamo_name     VARCHAR(255),              -- Nombre como aparece en DynamoDB (puede diferir)
    dynamo_tenant_id INTEGER UNIQUE,           -- ID numérico del tenant en DynamoDB
    is_active       BOOLEAN DEFAULT TRUE,
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_clients_odoo_id ON clients(odoo_id);
CREATE INDEX idx_clients_dynamo_name ON clients(dynamo_name);
CREATE INDEX idx_clients_dynamo_tenant_id ON clients(dynamo_tenant_id);

-- ============================================================================
-- TABLA: client_contacts (contactos de clientes — de Odoo, para enviar reportes)
-- ============================================================================

CREATE TABLE client_contacts (
    id              SERIAL PRIMARY KEY,
    client_id       INTEGER NOT NULL REFERENCES clients(id) ON DELETE CASCADE,
    name            VARCHAR(255) NOT NULL,
    whatsapp_number VARCHAR(20),              -- Número de WhatsApp para enviar reportes
    email           VARCHAR(255),
    odoo_user_id    INTEGER,                   -- ID del usuario en Odoo
    is_active       BOOLEAN DEFAULT TRUE,
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_client_contacts_client_id ON client_contacts(client_id);

-- ============================================================================
-- TABLA: shipments (guías/envíos — datos de DynamoDB + FedEx)
-- ============================================================================

CREATE TABLE shipments (
    id                      SERIAL PRIMARY KEY,
    tracking_number         VARCHAR(50) NOT NULL,
    client_id               INTEGER REFERENCES clients(id),
    client_name_raw         VARCHAR(255),          -- Nombre del cliente tal como viene de DynamoDB

    -- Estatus
    sonia_status            shipment_status DEFAULT 'unknown',
    fedex_status            VARCHAR(255),           -- Descripción original de FedEx
    fedex_status_code       VARCHAR(10),            -- Código de estatus FedEx

    -- Fechas
    label_creation_date     DATE,
    ship_date               DATE,
    delivery_date           DATE,
    estimated_delivery_date DATE,

    -- Ubicación
    destination_city        VARCHAR(100),
    destination_state       VARCHAR(50),
    destination_country     VARCHAR(10),
    origin_city             VARCHAR(100),
    origin_state            VARCHAR(50),
    origin_country          VARCHAR(10),

    -- Control
    is_delivered            BOOLEAN DEFAULT FALSE,  -- Flag para no reconsultar FedEx
    last_fedex_check        TIMESTAMPTZ,            -- Última vez que se consultó FedEx
    last_status_change      TIMESTAMPTZ,            -- Última vez que cambió el estatus
    fedex_check_count       INTEGER DEFAULT 0,      -- Cuántas veces se ha consultado

    -- Datos completos de FedEx (para referencia)
    raw_fedex_response      JSONB,

    -- Metadata de DynamoDB
    dynamo_data             JSONB,                  -- Datos originales de DynamoDB

    -- Timestamps
    created_at              TIMESTAMPTZ DEFAULT NOW(),
    updated_at              TIMESTAMPTZ DEFAULT NOW(),

    -- Constraint: tracking number único
    CONSTRAINT uq_tracking_number UNIQUE (tracking_number)
);

CREATE INDEX idx_shipments_tracking ON shipments(tracking_number);
CREATE INDEX idx_shipments_client_id ON shipments(client_id);
CREATE INDEX idx_shipments_is_delivered ON shipments(is_delivered);
CREATE INDEX idx_shipments_sonia_status ON shipments(sonia_status);
CREATE INDEX idx_shipments_last_fedex_check ON shipments(last_fedex_check);
CREATE INDEX idx_shipments_created_at ON shipments(created_at);

-- ============================================================================
-- TABLA: claims (reclamos)
-- ============================================================================

CREATE TABLE claims (
    id                      SERIAL PRIMARY KEY,
    claim_number            VARCHAR(20) NOT NULL UNIQUE,  -- Número legible: CLM-2026-0001
    tracking_number         VARCHAR(50),
    shipment_id             INTEGER REFERENCES shipments(id),
    client_id               INTEGER REFERENCES clients(id),
    client_name             VARCHAR(255),

    -- Tipo y descripción
    claim_type              claim_type NOT NULL,
    claim_type_other        VARCHAR(255),                  -- Si tipo = 'otro'
    description             TEXT,

    -- Estatus
    status                  claim_status DEFAULT 'nuevo',
    origin                  claim_origin DEFAULT 'manual', -- Preparado para Fase 2

    -- Asignación
    assigned_to_team        INTEGER REFERENCES portal_users(id),
    assigned_to_fedex_exec  INTEGER REFERENCES portal_users(id),

    -- Detección automática
    created_automatically   BOOLEAN DEFAULT FALSE,
    auto_detection_rule     VARCHAR(100),                  -- Qué regla lo detectó

    -- Resolución
    resolution_notes        TEXT,
    reimbursement_amount    DECIMAL(10, 2),
    reimbursement_currency  VARCHAR(3) DEFAULT 'USD',

    -- Timestamps
    created_at              TIMESTAMPTZ DEFAULT NOW(),
    updated_at              TIMESTAMPTZ DEFAULT NOW(),
    sent_to_fedex_at        TIMESTAMPTZ,
    resolved_at             TIMESTAMPTZ,
    closed_at               TIMESTAMPTZ
);

CREATE INDEX idx_claims_claim_number ON claims(claim_number);
CREATE INDEX idx_claims_tracking ON claims(tracking_number);
CREATE INDEX idx_claims_client_id ON claims(client_id);
CREATE INDEX idx_claims_status ON claims(status);
CREATE INDEX idx_claims_origin ON claims(origin);
CREATE INDEX idx_claims_assigned_fedex ON claims(assigned_to_fedex_exec);
CREATE INDEX idx_claims_created_at ON claims(created_at);

-- ============================================================================
-- TABLA: claim_recipients (datos del remitente/cliente final — NUNCA en Odoo)
-- ============================================================================

CREATE TABLE claim_recipients (
    id              SERIAL PRIMARY KEY,
    claim_id        INTEGER NOT NULL REFERENCES claims(id) ON DELETE CASCADE,

    -- Datos de contacto del cliente final
    name            VARCHAR(255),
    phone           VARCHAR(30),
    email           VARCHAR(255),

    -- Dirección
    address_line1   VARCHAR(255),
    address_line2   VARCHAR(255),
    city            VARCHAR(100),
    state           VARCHAR(50),
    zip_code        VARCHAR(20),
    country         VARCHAR(10) DEFAULT 'US',

    created_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_claim_recipients_claim_id ON claim_recipients(claim_id);
-- Índice para Fase 2: buscar por teléfono para WhatsApp Agent
CREATE INDEX idx_claim_recipients_phone ON claim_recipients(phone);

-- ============================================================================
-- TABLA: claim_history (historial de cambios del reclamo)
-- ============================================================================

CREATE TABLE claim_history (
    id              SERIAL PRIMARY KEY,
    claim_id        INTEGER NOT NULL REFERENCES claims(id) ON DELETE CASCADE,
    status_from     claim_status,
    status_to       claim_status NOT NULL,
    changed_by      INTEGER REFERENCES portal_users(id),
    changed_by_name VARCHAR(255),              -- Por si es sistema automático
    notes           TEXT,
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_claim_history_claim_id ON claim_history(claim_id);

-- ============================================================================
-- TABLA: claim_evidence (evidencia/archivos adjuntos)
-- ============================================================================

CREATE TABLE claim_evidence (
    id              SERIAL PRIMARY KEY,
    claim_id        INTEGER NOT NULL REFERENCES claims(id) ON DELETE CASCADE,
    filename        VARCHAR(255) NOT NULL,
    file_path       VARCHAR(500) NOT NULL,     -- Ruta en almacenamiento
    file_size       INTEGER,                    -- Tamaño en bytes
    mime_type       VARCHAR(100),
    uploaded_by     INTEGER REFERENCES portal_users(id),
    uploaded_by_name VARCHAR(255),
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_claim_evidence_claim_id ON claim_evidence(claim_id);

-- ============================================================================
-- TABLA: daily_run_logs (registro de ejecuciones diarias)
-- ============================================================================

CREATE TABLE daily_run_logs (
    id                  SERIAL PRIMARY KEY,
    run_date            DATE NOT NULL,
    started_at          TIMESTAMPTZ NOT NULL,
    completed_at        TIMESTAMPTZ,

    -- Métricas
    total_shipments_read    INTEGER DEFAULT 0,   -- Guías leídas de DynamoDB
    new_shipments           INTEGER DEFAULT 0,   -- Guías nuevas encontradas
    shipments_checked       INTEGER DEFAULT 0,   -- Guías consultadas en FedEx
    shipments_updated       INTEGER DEFAULT 0,   -- Guías con cambio de estatus
    shipments_delivered     INTEGER DEFAULT 0,   -- Guías que se marcaron entregadas
    claims_created          INTEGER DEFAULT 0,   -- Reclamos creados automáticamente
    reports_generated       INTEGER DEFAULT 0,   -- Reportes generados
    reports_sent            INTEGER DEFAULT 0,   -- Reportes enviados por WhatsApp
    alerts_sent             INTEGER DEFAULT 0,   -- Alertas enviadas

    -- Errores
    errors                  JSONB DEFAULT '[]',  -- Array de errores
    status                  run_status DEFAULT 'running',

    created_at              TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_daily_run_logs_run_date ON daily_run_logs(run_date);

-- ============================================================================
-- TABLA: anomaly_config (configuración de detección proactiva)
-- ============================================================================

CREATE TABLE anomaly_config (
    id              SERIAL PRIMARY KEY,
    rule_name       VARCHAR(100) NOT NULL UNIQUE,
    description     TEXT,
    is_enabled      BOOLEAN DEFAULT TRUE,
    threshold_days  INTEGER,                    -- Umbral en días (si aplica)
    config          JSONB DEFAULT '{}',         -- Configuración adicional
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);

-- Insertar reglas por defecto
INSERT INTO anomaly_config (rule_name, description, is_enabled, threshold_days) VALUES
    ('exception_detected', 'Paquete con excepción de entrega reportada por FedEx', TRUE, NULL),
    ('transit_too_long', 'Paquete en tránsito por más de X días hábiles', TRUE, 7),
    ('returned_to_sender', 'Paquete devuelto a origen', TRUE, NULL),
    ('delivery_attempted_stuck', 'Intento de entrega sin éxito por más de X días', TRUE, 2),
    ('customs_too_long', 'Paquete en aduanas por más de X días hábiles', TRUE, 5),
    ('label_no_movement', 'Label creada sin movimiento por más de X días', TRUE, 5);

-- ============================================================================
-- TABLA: tenant_mapping (mapeo manual de tenant_id de DynamoDB a clientes)
-- Esta tabla se puede poblar desde el documento "WHATSAPP BBDD" de Odoo
-- ============================================================================

CREATE TABLE tenant_mapping (
    id              SERIAL PRIMARY KEY,
    dynamo_tenant_id INTEGER NOT NULL UNIQUE,  -- ID del tenant en DynamoDB
    client_id       INTEGER REFERENCES clients(id),
    client_name     VARCHAR(255) NOT NULL,      -- Nombre del cliente/marca
    odoo_company_id INTEGER,                    -- ID de la compañía en Odoo
    whatsapp_numbers TEXT[],                    -- Números de WhatsApp del documento BBDD
    is_active       BOOLEAN DEFAULT TRUE,
    notes           TEXT,
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_tenant_mapping_dynamo_id ON tenant_mapping(dynamo_tenant_id);
CREATE INDEX idx_tenant_mapping_client_id ON tenant_mapping(client_id);

-- Insertar mapeos conocidos (actualizarlos con datos reales del documento "WHATSAPP BBDD" de Odoo)
INSERT INTO tenant_mapping (dynamo_tenant_id, client_name) VALUES
    (1, 'Pendiente - Tenant 1'),
    (2, 'Pendiente - Tenant 2'),
    (14, 'Pendiente - Tenant 14'),
    (15, 'Pendiente - Tenant 15'),
    (16, 'Pendiente - Tenant 16'),
    (17, 'Pendiente - Tenant 17'),
    (18, 'Pendiente - Tenant 18'),
    (19, 'Pendiente - Tenant 19'),
    (21, 'Pendiente - Tenant 21'),
    (28, 'Pendiente - Tenant 28');

-- ============================================================================
-- FUNCIÓN: Generar número de reclamo automáticamente
-- ============================================================================

CREATE OR REPLACE FUNCTION generate_claim_number()
RETURNS TRIGGER AS $$
DECLARE
    year_str VARCHAR(4);
    next_num INTEGER;
BEGIN
    year_str := TO_CHAR(NOW(), 'YYYY');
    SELECT COALESCE(MAX(
        CAST(SUBSTRING(claim_number FROM 10) AS INTEGER)
    ), 0) + 1
    INTO next_num
    FROM claims
    WHERE claim_number LIKE 'CLM-' || year_str || '-%';

    NEW.claim_number := 'CLM-' || year_str || '-' || LPAD(next_num::TEXT, 4, '0');
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_generate_claim_number
    BEFORE INSERT ON claims
    FOR EACH ROW
    WHEN (NEW.claim_number IS NULL OR NEW.claim_number = '')
    EXECUTE FUNCTION generate_claim_number();

-- ============================================================================
-- FUNCIÓN: Actualizar updated_at automáticamente
-- ============================================================================

CREATE OR REPLACE FUNCTION update_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_clients_updated_at
    BEFORE UPDATE ON clients FOR EACH ROW EXECUTE FUNCTION update_updated_at();

CREATE TRIGGER trg_shipments_updated_at
    BEFORE UPDATE ON shipments FOR EACH ROW EXECUTE FUNCTION update_updated_at();

CREATE TRIGGER trg_claims_updated_at
    BEFORE UPDATE ON claims FOR EACH ROW EXECUTE FUNCTION update_updated_at();

CREATE TRIGGER trg_portal_users_updated_at
    BEFORE UPDATE ON portal_users FOR EACH ROW EXECUTE FUNCTION update_updated_at();

CREATE TRIGGER trg_tenant_mapping_updated_at
    BEFORE UPDATE ON tenant_mapping FOR EACH ROW EXECUTE FUNCTION update_updated_at();

-- ============================================================================
-- FIN DE LA MIGRACIÓN INICIAL
-- Orden de creación: ENUMs → portal_users → clients → client_contacts →
-- shipments → claims → claim_recipients → claim_history → claim_evidence →
-- daily_run_logs → anomaly_config → tenant_mapping → funciones y triggers
-- ============================================================================
