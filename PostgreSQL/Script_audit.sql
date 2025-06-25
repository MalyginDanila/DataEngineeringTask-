-- Создание структуры таблиц.

CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    name TEXT,
    email TEXT,
    role TEXT,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE users_audit (
    id SERIAL PRIMARY KEY,
    user_id INTEGER,
    changed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    changed_by TEXT,
    field_changed TEXT,
    old_value TEXT,
    new_value TEXT
);

-- Создание функции логирования. Функция делает 1 запись на каждое изменение.

CREATE OR REPLACE FUNCTION log_users_changes()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.name IS DISTINCT FROM OLD.name THEN
        INSERT INTO users_audit(user_id, changed_at, changed_by, field_changed, old_value, new_value)
        VALUES (OLD.id, now(), current_user, 'name', OLD.name, NEW.name);
    END IF;

    IF NEW.email IS DISTINCT FROM OLD.email THEN
        INSERT INTO users_audit(user_id, changed_at, changed_by, field_changed, old_value, new_value)
        VALUES (OLD.id, now(), current_user, 'email', OLD.email, NEW.email);
    END IF;

    IF NEW.role IS DISTINCT FROM OLD.role THEN
        INSERT INTO users_audit(user_id, changed_at, changed_by, field_changed, old_value, new_value)
        VALUES (OLD.id, now(), current_user, 'role', OLD.role, NEW.role);
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Создание триггера.

CREATE TRIGGER trigger_log_users_changes
AFTER UPDATE ON users
FOR EACH ROW
EXECUTE FUNCTION log_users_changes();

-- Установка pg_cron

CREATE EXTENSION IF NOT exists pg_cron;

-- Экспорт данных в csv

create or replace function export_to_csv()
returns void as $$
declare
	export_path text;
begin
	export_path := '/tmp/user_audit_export_' || to_char(current_date, 'YYYY-MM-DD') || '.csv';

	execute format($f$
		copy (
			select * from users_audit ua 
			where changed_at >= current_date - interval '1 day' and changed_at < current_date
			order by changed_at
		) to %L with csv header
	$f$, export_path);
end;
$$ language plpgsql;

-- Установка планировщика

select cron.schedule('0 3 * * *', $$select export_to_csv():$$);


	
