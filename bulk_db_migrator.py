#!/usr/bin/python3
import argparse
import subprocess
import os
import datetime
import glob
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT


class BulkDBMigrator:
    def __init__(self, source_host, source_user, source_password,
                 dest_host, dest_user, dest_password, port=5432, use_inserts=False):
        self.source_host = source_host
        self.source_user = source_user
        self.source_password = source_password
        self.dest_host = dest_host
        self.dest_user = dest_user
        self.dest_password = dest_password
        self.port = port
        self.use_inserts = use_inserts
        self.temp_dir = './migration_temp'

        # Create temp directory for migration files
        os.makedirs(self.temp_dir, exist_ok=True)

        # Find PostgreSQL tools
        self.pg_dump_path, self.psql_path = self.find_postgresql_tools()

    def find_postgresql_tools(self):
        """Find PostgreSQL tools (pg_dump, psql) on the system"""
        # Common PostgreSQL installation paths on Windows
        common_paths = [
            r"C:\Program Files\PostgreSQL\*\bin",
            r"C:\Program Files (x86)\PostgreSQL\*\bin",
            r"C:\PostgreSQL\*\bin",
            "/usr/bin",
            "/usr/local/bin",
            "/opt/postgresql/*/bin"
        ]

        pg_dump_path = None
        psql_path = None

        # First, try to find in PATH
        try:
            result = subprocess.run(['pg_dump', '--version'], capture_output=True, text=True)
            if result.returncode == 0:
                pg_dump_path = 'pg_dump'
                psql_path = 'psql'
                print("✓ Found PostgreSQL tools in system PATH")
                return pg_dump_path, psql_path
        except:
            pass

        # Search in common installation directories
        for path_pattern in common_paths:
            try:
                for path in glob.glob(path_pattern):
                    pg_dump_candidate = os.path.join(path, 'pg_dump.exe' if os.name == 'nt' else 'pg_dump')
                    psql_candidate = os.path.join(path, 'psql.exe' if os.name == 'nt' else 'psql')

                    if os.path.exists(pg_dump_candidate) and os.path.exists(psql_candidate):
                        pg_dump_path = pg_dump_candidate
                        psql_path = psql_candidate
                        print(f"✓ Found PostgreSQL tools at: {path}")
                        return pg_dump_path, psql_path
            except:
                continue

        print("⚠ PostgreSQL tools not found. Please ensure PostgreSQL is installed and in PATH.")
        print("Common locations to check:")
        print("  - C:\\Program Files\\PostgreSQL\\15\\bin")
        print("  - C:\\Program Files\\PostgreSQL\\14\\bin")
        return None, None

    def get_connection(self, use_destination=False, database='postgres'):
        """Get database connection to source or destination"""
        try:
            if use_destination:
                conn = psycopg2.connect(
                    host=self.dest_host,
                    port=self.port,
                    database=database,
                    user=self.dest_user,
                    password=self.dest_password
                )
            else:
                conn = psycopg2.connect(
                    host=self.source_host,
                    port=self.port,
                    database=database,
                    user=self.source_user,
                    password=self.source_password
                )
            return conn
        except psycopg2.Error as e:
            print(f"Connection error: {e}")
            return None

    def test_connections(self):
        """Test both source and destination connections"""
        print("Testing connections...")

        # Check if PostgreSQL tools are available
        if not self.pg_dump_path or not self.psql_path:
            print("✗ PostgreSQL tools (pg_dump/psql) not found!")
            print("Please ensure PostgreSQL is installed and accessible.")
            return False
        else:
            print(f"✓ Using pg_dump: {self.pg_dump_path}")
            print(f"✓ Using psql: {self.psql_path}")

        # Show dump format
        dump_format = "INSERT statements" if self.use_inserts else "COPY statements"
        print(f"✓ Dump format: {dump_format}")

        # Test source
        source_conn = self.get_connection(use_destination=False)
        if source_conn:
            print(f"✓ Source connection successful ({self.source_host})")
            source_conn.close()
        else:
            print(f"✗ Source connection failed ({self.source_host})")
            return False

        # Test destination
        dest_conn = self.get_connection(use_destination=True)
        if dest_conn:
            print(f"✓ Destination connection successful ({self.dest_host})")
            dest_conn.close()
        else:
            print(f"✗ Destination connection failed ({self.dest_host})")
            return False

        return True

    def list_databases(self, use_destination=False, exclude_system=True):
        """List databases on source or destination server"""
        conn = self.get_connection(use_destination=use_destination)
        if not conn:
            return []

        try:
            conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            cursor = conn.cursor()

            if exclude_system:
                cursor.execute("""
                    SELECT datname FROM pg_database 
                    WHERE datistemplate = false 
                    AND datname NOT IN ('postgres', 'template0', 'template1')
                    ORDER BY datname
                """)
            else:
                cursor.execute("""
                    SELECT datname FROM pg_database 
                    WHERE datistemplate = false 
                    ORDER BY datname
                """)

            databases = [row[0] for row in cursor.fetchall()]
            cursor.close()
            return databases

        except psycopg2.Error as e:
            print(f"Error listing databases: {e}")
            return []
        finally:
            conn.close()

    def database_exists(self, database_name, use_destination=False):
        """Check if database exists"""
        conn = self.get_connection(use_destination=use_destination)
        if not conn:
            return False

        try:
            cursor = conn.cursor()
            cursor.execute("SELECT 1 FROM pg_database WHERE datname = %s", (database_name,))
            exists = cursor.fetchone() is not None
            cursor.close()
            return exists
        except psycopg2.Error:
            return False
        finally:
            conn.close()

    def create_database(self, database_name):
        """Create database on destination server"""
        conn = self.get_connection(use_destination=True)
        if not conn:
            return False

        try:
            conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            cursor = conn.cursor()
            cursor.execute(f'CREATE DATABASE "{database_name}"')
            cursor.close()
            return True
        except psycopg2.Error as e:
            print(f"Error creating database '{database_name}': {e}")
            return False
        finally:
            conn.close()

    def migrate_single_database(self, database_name, overwrite=False):
        """Migrate a single database from source to destination"""
        print(f"\n--- Migrating database: {database_name} ---")

        # Check if database exists on destination
        if self.database_exists(database_name, use_destination=True):
            if not overwrite:
                print(f"⚠ Database '{database_name}' exists on destination. Use --overwrite to replace.")
                return False
            else:
                print(f"! Overwriting existing database '{database_name}'")

        # Create backup file path
        timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
        backup_file = os.path.join(self.temp_dir, f"{database_name}_{timestamp}.sql")

        try:
            # Step 1: Backup from source
            dump_type = "INSERT" if self.use_inserts else "COPY"
            print(f"1. Backing up from source (using {dump_type} format)...")
            if not self._backup_database(database_name, backup_file):
                return False

            # Step 2: Create database on destination (if needed)
            print(f"2. Creating database on destination...")
            if not self.database_exists(database_name, use_destination=True):
                if not self.create_database(database_name):
                    return False
            else:
                print(f"   Database already exists")

            # Step 3: Restore to destination
            print(f"3. Restoring to destination...")
            if not self._restore_database(database_name, backup_file):
                return False

            # Step 4: Cleanup
            print(f"4. Cleaning up temporary files...")
            os.remove(backup_file)

            print(f"✓ Successfully migrated database '{database_name}'")
            return True

        except Exception as e:
            print(f"✗ Migration failed for '{database_name}': {e}")
            # Cleanup on failure
            if os.path.exists(backup_file):
                os.remove(backup_file)
            return False

    def _backup_database(self, database_name, backup_file):
        """Internal method to backup database"""
        if not self.pg_dump_path:
            print("   ✗ pg_dump not found")
            return False

        try:
            cmd = [
                self.pg_dump_path,
                '-h', self.source_host,
                '-p', str(self.port),
                '-U', self.source_user,
                '-d', database_name,
                '-f', backup_file,
                '--no-password',
                '--verbose'
            ]

            # Add INSERT option if requested
            if self.use_inserts:
                cmd.append('--inserts')  # Use INSERT statements instead of COPY
                # Alternative: use --column-inserts for INSERT with column names
                # cmd.append('--column-inserts')

            env = os.environ.copy()
            env['PGPASSWORD'] = self.source_password

            result = subprocess.run(cmd, env=env, capture_output=True, text=True)

            if result.returncode == 0:
                file_size = os.path.getsize(backup_file)
                format_note = " (INSERT format)" if self.use_inserts else " (COPY format)"
                print(f"   ✓ Backup created ({file_size} bytes){format_note}")
                return True
            else:
                print(f"   ✗ Backup failed: {result.stderr}")
                return False

        except Exception as e:
            print(f"   ✗ Backup error: {e}")
            return False

    def _restore_database(self, database_name, backup_file):
        """Internal method to restore database"""
        if not self.psql_path:
            print("   ✗ psql not found")
            return False

        try:
            cmd = [
                self.psql_path,
                '-h', self.dest_host,
                '-p', str(self.port),
                '-U', self.dest_user,
                '-d', database_name,
                '-f', backup_file,
                '--no-password'
            ]

            env = os.environ.copy()
            env['PGPASSWORD'] = self.dest_password

            result = subprocess.run(cmd, env=env, capture_output=True, text=True)

            if result.returncode == 0:
                print(f"   ✓ Restore completed")
                return True
            else:
                print(f"   ✗ Restore failed: {result.stderr}")
                return False

        except Exception as e:
            print(f"   ✗ Restore error: {e}")
            return False

    def migrate_all_databases(self, exclude_databases=None, overwrite=False):
        """Migrate all databases from source to destination"""
        if exclude_databases is None:
            exclude_databases = []

        print("=== BULK DATABASE MIGRATION ===")
        print(f"Source: {self.source_host}")
        print(f"Destination: {self.dest_host}")
        dump_format = "INSERT statements" if self.use_inserts else "COPY statements"
        print(f"Format: {dump_format}")

        # Get list of source databases
        source_databases = self.list_databases(use_destination=False)
        if not source_databases:
            print("No databases found on source server.")
            return

        # Filter out excluded databases
        databases_to_migrate = [db for db in source_databases if db not in exclude_databases]

        print(f"\nDatabases to migrate: {len(databases_to_migrate)}")
        for db in databases_to_migrate:
            print(f"  - {db}")

        if exclude_databases:
            print(f"\nExcluded databases: {exclude_databases}")

        # Confirm migration
        if not self._confirm_migration(databases_to_migrate):
            print("Migration cancelled.")
            return

        # Migrate each database
        successful = 0
        failed = 0

        for database in databases_to_migrate:
            if self.migrate_single_database(database, overwrite=overwrite):
                successful += 1
            else:
                failed += 1

        # Summary
        print(f"\n=== MIGRATION SUMMARY ===")
        print(f"✓ Successful: {successful}")
        print(f"✗ Failed: {failed}")
        print(f"Total: {len(databases_to_migrate)}")

    def _confirm_migration(self, databases):
        """Ask user to confirm migration"""
        format_note = " using INSERT statements" if self.use_inserts else " using COPY statements"
        print(f"\nAbout to migrate {len(databases)} databases{format_note}.")
        if self.use_inserts:
            print("⚠ Note: INSERT format is slower but more portable than COPY format.")
        response = input("Continue? (y/N): ").strip().lower()
        return response in ['y', 'yes']

    def show_comparison(self):
        """Show comparison between source and destination servers"""
        print("=== SERVER COMPARISON ===")

        print(f"\nSource server ({self.source_host}):")
        source_dbs = self.list_databases(use_destination=False)
        for db in source_dbs:
            print(f"  - {db}")

        print(f"\nDestination server ({self.dest_host}):")
        dest_dbs = self.list_databases(use_destination=True)
        for db in dest_dbs:
            print(f"  - {db}")

        # Show differences
        only_in_source = set(source_dbs) - set(dest_dbs)
        only_in_dest = set(dest_dbs) - set(source_dbs)
        common = set(source_dbs) & set(dest_dbs)

        if only_in_source:
            print(f"\nOnly in source ({len(only_in_source)}):")
            for db in sorted(only_in_source):
                print(f"  - {db}")

        if only_in_dest:
            print(f"\nOnly in destination ({len(only_in_dest)}):")
            for db in sorted(only_in_dest):
                print(f"  - {db}")

        if common:
            print(f"\nCommon databases ({len(common)}):")
            for db in sorted(common):
                print(f"  - {db}")


def main():
    parser = argparse.ArgumentParser(description='Bulk PostgreSQL Database Migration Tool')

    # Source server settings
    parser.add_argument('--source-host', required=True, help='Source database host')
    parser.add_argument('--source-user', default='postgres', help='Source database user')
    parser.add_argument('--source-password', required=True, help='Source database password')

    # Destination server settings
    parser.add_argument('--dest-host', required=True, help='Destination database host')
    parser.add_argument('--dest-user', default='postgres', help='Destination database user')
    parser.add_argument('--dest-password', required=True, help='Destination database password')

    # Common settings
    parser.add_argument('--port', type=int, default=5432, help='Database port (same for both servers)')
    parser.add_argument('--use-inserts', action='store_true',
                       help='Use INSERT statements instead of COPY (slower but more portable)')

    # Actions
    subparsers = parser.add_subparsers(dest='action', help='Available actions')

    # Test connections
    subparsers.add_parser('test', help='Test connections to both servers')

    # Compare servers
    subparsers.add_parser('compare', help='Compare databases between servers')

    # Migrate single database
    migrate_single = subparsers.add_parser('migrate-single', help='Migrate a single database')
    migrate_single.add_argument('database', help='Database name to migrate')
    migrate_single.add_argument('--overwrite', action='store_true', help='Overwrite if exists on destination')

    # Migrate all databases
    migrate_all = subparsers.add_parser('migrate-all', help='Migrate all databases')
    migrate_all.add_argument('--exclude', nargs='*', default=[], help='Databases to exclude from migration')
    migrate_all.add_argument('--overwrite', action='store_true', help='Overwrite existing databases on destination')

    args = parser.parse_args()

    if not args.action:
        parser.print_help()
        return

    # Create migrator
    migrator = BulkDBMigrator(
        source_host=args.source_host,
        source_user=args.source_user,
        source_password=args.source_password,
        dest_host=args.dest_host,
        dest_user=args.dest_user,
        dest_password=args.dest_password,
        port=args.port,
        use_inserts=args.use_inserts
    )

    # Execute actions
    if args.action == 'test':
        if migrator.test_connections():
            print("✓ All connections successful!")
        else:
            print("✗ Connection test failed!")

    elif args.action == 'compare':
        if migrator.test_connections():
            migrator.show_comparison()

    elif args.action == 'migrate-single':
        if migrator.test_connections():
            migrator.migrate_single_database(args.database, overwrite=args.overwrite)

    elif args.action == 'migrate-all':
        if migrator.test_connections():
            migrator.migrate_all_databases(exclude_databases=args.exclude, overwrite=args.overwrite)


if __name__ == '__main__':
    main()