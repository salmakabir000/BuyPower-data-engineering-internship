import sys, tempfile, unittest
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
import mini_migrate

class MigrationTests(unittest.TestCase):
    def test_split_up_down(self):
        up,down=mini_migrate.split_sql('-- UP\nCREATE TABLE x(id INT);\n-- DOWN\nDROP TABLE x;')
        self.assertEqual(up,'CREATE TABLE x(id INT);'); self.assertEqual(down,'DROP TABLE x;')
    def test_missing_down_rejected(self):
        with self.assertRaises(mini_migrate.MigrationError): mini_migrate.split_sql('-- UP\nSELECT 1;')
    def test_checksum_changes_when_file_changes(self):
        self.assertNotEqual(mini_migrate.checksum('SELECT 1'),mini_migrate.checksum('SELECT 2'))
    def test_files_sorted_by_version(self):
        with tempfile.TemporaryDirectory() as folder:
            p=Path(folder)
            for name in ['V002__second.sql','V001__first.sql']:
                (p/name).write_text('-- UP\nSELECT 1;\n-- DOWN\nSELECT 1;')
            self.assertEqual([x['version'] for x in mini_migrate.load_migrations() if False], [])
            # Test ordering through the actual discovery function's directory argument.
            original=mini_migrate.MIGRATIONS
            try:
                mini_migrate.MIGRATIONS=p
                self.assertEqual([x['version'] for x in mini_migrate.load_migrations()],['V001','V002'])
            finally: mini_migrate.MIGRATIONS=original
    def test_out_of_order_history_rejected(self):
        migrations=[{'version':'V001','checksum':'a'},{'version':'V002','checksum':'b'},{'version':'V003','checksum':'c'}]
        rows=[('V001','one','a'),('V003','three','c')]
        with self.assertRaisesRegex(mini_migrate.MigrationError,'Out-of-order'):
            mini_migrate.validate_history(migrations,rows)
    def test_checksum_mismatch_rejected(self):
        with self.assertRaisesRegex(mini_migrate.MigrationError,'Checksum mismatch'):
            mini_migrate.validate_history([{'version':'V001','checksum':'new'}],[('V001','one','old')])

if __name__=='__main__': unittest.main()
