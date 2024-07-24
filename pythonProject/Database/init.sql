-- Tags table
DROP
  TABLE IF EXISTS TAGS CASCADE;
CREATE TABLE IF NOT EXISTS TAGS (
  ID SERIAL PRIMARY KEY,
  TAG_NAME VARCHAR(255),
  IS_BATCH_NAME BOOLEAN DEFAULT FALSE,
  IS_LOT_SIZE BOOLEAN DEFAULT FALSE,
  IS_CONTROL_TAG BOOLEAN DEFAULT FALSE
);

-- Tag Configuration Table
DROP
  TABLE IF EXISTS TAG_CONFIGURATIONS CASCADE;
CREATE TABLE IF NOT EXISTS TAG_CONFIGURATIONS (
  ID SERIAL PRIMARY KEY,
  MASTER_TAG_ID INT,
  SLAVE_TAG_ID INT
);

ALTER TABLE
  TAG_CONFIGURATIONS
ADD
  CONSTRAINT FK_TAG_CONFIGURATIONS_MASTER_TAG FOREIGN KEY (MASTER_TAG_ID) REFERENCES TAGS(ID);
ALTER TABLE
  TAG_CONFIGURATIONS
ADD
  CONSTRAINT FK_TAG_CONFIGURATIONS_SLAVE_TAG FOREIGN KEY (SLAVE_TAG_ID) REFERENCES TAGS(ID);
CREATE UNIQUE INDEX uq_tag_configurations ON TAG_CONFIGURATIONS USING btree(MASTER_TAG_ID, SLAVE_TAG_ID);

-- Data Partition Table
CREATE TABLE TAG_READING_PARTITIONS(
    ID SERIAL PRIMARY KEY,
    START_TIME TIMESTAMP NOT NULL,
    END_TIME TIMESTAMP NULL
);

-- Insert Tag Data
-- Batch Tag
INSERT INTO tags (
  tag_name, is_batch_name, is_lot_size,
  is_control_tag
)
VALUES
  ('2570/5096/SON', true, false, false);
-- Lot size Tag
INSERT INTO tags (
  tag_name, is_batch_name, is_lot_size,
  is_control_tag
)
VALUES
  ('OEE_DB2PI/DSF4/Data.LotSize', false, true, false);
-- Master Control Tag
INSERT INTO tags (
  tag_name, is_batch_name, is_lot_size,
  is_control_tag
)
VALUES
  ('2570/Ma_B.SLV.Counter.Counter0200', false, false, true);
-- Master Error Tag
INSERT INTO tags (
  tag_name, is_batch_name, is_lot_size,
  is_control_tag
)
VALUES
  ('2570/Ma_B.SLV.Counter.Counter0202', false, false, false);

-- Insert Sample Tag Configuration Data
INSERT INTO TAG_CONFIGURATIONS(MASTER_TAG_ID, SLAVE_TAG_ID)
VALUES
  (
    (
      SELECT
        ID
      FROM
        TAGS
      WHERE
        TAG_NAME = '2570/Ma_B.SLV.Counter.Counter0200'
    ),
    (
      SELECT
        ID
      FROM
        TAGS
      WHERE
        TAG_NAME = '2570/Ma_B.SLV.Counter.Counter0202'
    )
  );