# Bulk Database Migrator

Bulk Database Migrator allows you to test connections, compare databases, and migrate all or specific databases between servers efficiently.

## Features

1. Test connections between source and destination servers.
2. Compare the databases available on source and destination servers.
3. Migrate all databases from the source to the destination server.
4. Migrate a single database from the source to destination with optional overwriting.
5. Optionally exclude specific databases during the migration process.

---

## Usage

### 1. Test Connections to Both Servers

Use the `test` command to ensure the source and destination servers are accessible and the credentials are correct:

```bash
python bulk_db_migrator.py --source-host old-server --source-password oldpass --dest-host new-server --dest-password newpass test
```

---

### 2. Compare Databases on Each Server

The `compare` command helps identify the differences between the databases on the source and destination servers:

```bash
python bulk_db_migrator.py --source-host old-server --source-password oldpass --dest-host new-server --dest-password newpass compare
```

---

### 3. Migrate All Databases

To migrate **all databases** from the source server to the destination server, use the `migrate-all` command:

```bash
python bulk_db_migrator.py --source-host old-server --source-password oldpass --dest-host new-server --dest-password newpass --use-inserts migrate-all
```

---

### 4. Migrate All Databases Except Specific Ones

To migrate all **except specific databases** (e.g., system or default databases like `postgres`, `template0`, and `template1`), use the `--exclude` flag:

```bash
python bulk_db_migrator.py --source-host old-server --source-password oldpass --dest-host new-server --dest-password newpass --use-inserts migrate-all --exclude postgres template0 template1
```

---

### 5. Migrate a Single Database

Use the `migrate-single` command to migrate an individual database from the source to the destination server. Use the `--overwrite` flag to replace the database on the destination server if it already exists:

```bash
python bulk_db_migrator.py --source-host old-server --source-password oldpass --dest-host new-server --dest-password newpass migrate-single example_database --overwrite
```

---

## Notes

- Replace `old-server`, `oldpass`, `new-server`, and `newpass` with the actual host addresses and passwords for your source and destination servers.
- This script requires the following to be installed:
  - **Python dependencies**:
    - `psycopg2`: For interacting with PostgreSQL.
    - `argparse`: For argument parsing.
  - **PostgreSQL CLI tools**:
    - `pg_dump`: For exporting databases from the source server.
    - `psql`: For importing databases to the destination server.
- Use the `--use-inserts` flag to switch from the default (faster) `COPY` format to the slower but more portable `INSERT` statements during migration.
- Be cautious while running commands like `migrate-all` or `migrate-single --overwrite`, as they can overwrite data on the destination server.

---

## Contributing

If you'd like to contribute to this project, feel free to fork the repository and submit a pull request. Make sure to follow the coding guidelines and add appropriate documentation for any new features.

---

## License

This project is licensed under the [MIT License](LICENSE).