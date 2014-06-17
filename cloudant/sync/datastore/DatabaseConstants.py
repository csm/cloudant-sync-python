import uuid

SCHEMA_VERSION_3 = [
    """  CREATE TABLE docs (
    doc_id INTEGER PRIMARY KEY,
    docid TEXT UNIQUE NOT NULL);""",
    "  CREATE INDEX docs_docid ON docs(docid)",
    """  CREATE TABLE revs (
    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    doc_id INTEGER NOT NULL REFERENCES docs(doc_id) ON DELETE CASCADE,
    parent INTEGER REFERENCES revs(sequence) ON DELETE SET NULL,
    current BOOLEAN,
    deleted BOOLEAN DEFAULT 0,
    available BOOLEAN DEFAULT 1,
    revid TEXT NOT NULL,
    json BLOB);""",
    "  CREATE INDEX revs_by_id ON revs(revid, doc_id); ",
    "  CREATE INDEX revs_current ON revs(doc_id, current); ",
    "  CREATE INDEX revs_parent ON revs(parent); ",
    """  CREATE TABLE localdocs (
    docid TEXT UNIQUE NOT NULL,
    revid TEXT NOT NULL,
    json BLOB);""",
    "  CREATE INDEX localdocs_by_docid ON localdocs(docid); ",
    """  CREATE TABLE views (
    view_id INTEGER PRIMARY KEY,
    name TEXT UNIQUE NOT NULL,
    version TEXT,
    lastsequence INTEGER DEFAULT 0);""",
    "  CREATE INDEX views_by_name ON views(name); ",
    """  CREATE TABLE maps (
    view_id INTEGER NOT NULL REFERENCES views(view_id) ON DELETE CASCADE,
    sequence INTEGER NOT NULL REFERENCES revs(sequence) ON DELETE CASCADE,
    key TEXT NOT NULL,
    collation_key BLOB NOT NULL,
    value TEXT); """,
    "  CREATE INDEX maps_keys on maps(view_id, collation_key); ",
    """  CREATE TABLE attachments (
    sequence INTEGER NOT NULL REFERENCES revs(sequence) ON DELETE CASCADE,
    filename TEXT NOT NULL,
    key BLOB NOT NULL,
    type TEXT,
    length INTEGER NOT NULL,
    revpos INTEGER DEFAULT 0); """,
    "  CREATE INDEX attachments_by_sequence on attachments(sequence, filename); ",
    """  CREATE TABLE replicators (
    remote TEXT NOT NULL,
    startPush BOOLEAN,
    last_sequence TEXT,
    UNIQUE (remote, startPush)); """
]

SCHEMA_VERSION_4 = [
    "CREATE TABLE info ( " +
    "    key TEXT PRIMARY KEY, " +
    "    value TEXT); ",
    "INSERT INTO INFO (key, value) VALUES ('privateUUID', '%s'); ",
    "INSERT INTO INFO (key, value) VALUES ('publicUUID',  '%s'); "
]

def getSCHEMA_VERSION_4():
    return [
        SCHEMA_VERSION_4[0],
        SCHEMA_VERSION_4[1] % str(uuid.uuid4()).replace('-', ''),
        SCHEMA_VERSION_4[2] % str(uuid.uuid4()).replace('-', '')
    ]