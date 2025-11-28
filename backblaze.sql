DROP TABLE IF EXISTS drive_models;

CREATE TABLE drive_models (
  drive_model_id_pk UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  drive_model_name_smart VARCHAR UNIQUE NOT NULL,
  drive_model_size_tb DECIMAL NOT NULL
);

CREATE INDEX drive_models_drive_model_name_smart_idx ON drive_models(drive_model_name_smart);

DROP TABLE IF EXISTS drives;

CREATE TABLE drives (
  drive_id_pk UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  drive_model_fk UUID REFERENCES drive_models(drive_model_id_pk) NOT NULL,
  drive_serial_number VARCHAR NOT NULL,

  UNIQUE (drive_model_fk, drive_serial_number) 
);

CREATE INDEX drives_drive_model_fk_idx ON drives(drive_model_fk);
CREATE INDEX drives_drive_serial_number_idx ON drives(drive_serial_number);
