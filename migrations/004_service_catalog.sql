-- Свод Оснований для Созидания — реестр услуг и товаров (валюта: КВАЗИ / QZ)

CREATE TABLE IF NOT EXISTS service_categories (
    id SERIAL PRIMARY KEY,
    code TEXT UNIQUE NOT NULL,
    name TEXT NOT NULL,
    description TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS service_catalog (
    id SERIAL PRIMARY KEY,
    code TEXT UNIQUE NOT NULL,
    name TEXT NOT NULL,
    description TEXT,
    category_id INTEGER REFERENCES service_categories(id),
    base_price BIGINT NOT NULL,
    min_quantity BIGINT DEFAULT 1,
    max_quantity BIGINT DEFAULT 100,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_service_catalog_category ON service_catalog(category_id);
CREATE INDEX IF NOT EXISTS idx_service_catalog_active ON service_catalog(is_active);

ALTER TABLE offers ADD COLUMN IF NOT EXISTS svod_code TEXT;
CREATE INDEX IF NOT EXISTS idx_offers_svod_code ON offers(svod_code);

-- Базовые категории и услуги для dev/smoke-тестов
INSERT INTO service_categories (code, name, description)
VALUES ('IT', 'Information Technology', 'IT-услуги и разработка')
ON CONFLICT (code) DO NOTHING;

INSERT INTO service_catalog (code, name, description, category_id, base_price, min_quantity, max_quantity)
SELECT
    'WEB_DEV',
    'Web Development',
    'Разработка веб-приложений',
    c.id,
    100,
    1,
    50
FROM service_categories c
WHERE c.code = 'IT'
ON CONFLICT (code) DO NOTHING;
