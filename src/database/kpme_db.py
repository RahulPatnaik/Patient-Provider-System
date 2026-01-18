"""
KPME Database Manager - SQLite database for Karnataka healthcare establishments

Loads and manages 5 CSV files:
- KPME_FULL_DATA.csv (main establishments)
- KPME_STAFF.csv (staff with fees)
- KPME_SPECIALTIES.csv (specialties)
- KPME_CERTIFICATES.csv (certificates)
- KPME_TREATMENTS.csv (treatment charges)

Provides fast querying for provider validation.
"""

import sqlite3
import pandas as pd
from pathlib import Path
from typing import Optional, Dict, Any, List
import logging

logger = logging.getLogger(__name__)


class KPMEDatabase:
    """SQLite database manager for KPME Karnataka healthcare data."""

    def __init__(self, db_path: Optional[str] = None, csv_dir: Optional[str] = None):
        """
        Initialize KPME database.

        Args:
            db_path: Path to SQLite database file (default: src/database/kpme.db)
            csv_dir: Directory containing CSV files (default: dataset/)
        """
        # Set paths
        if db_path is None:
            db_path = Path(__file__).parent / "kpme.db"
        else:
            db_path = Path(db_path)

        if csv_dir is None:
            csv_dir = Path(__file__).parent.parent.parent / "dataset"
        else:
            csv_dir = Path(csv_dir)

        self.db_path = db_path
        self.csv_dir = csv_dir
        self.conn: Optional[sqlite3.Connection] = None

        logger.info(f"KPME Database initialized: {self.db_path}")

    def connect(self) -> sqlite3.Connection:
        """Connect to database (creates if doesn't exist)."""
        if self.conn is None:
            self.conn = sqlite3.connect(str(self.db_path))
            self.conn.row_factory = sqlite3.Row  # Return rows as dicts
        return self.conn

    def close(self):
        """Close database connection."""
        if self.conn:
            self.conn.close()
            self.conn = None

    def load_from_csv(self, force_reload: bool = False):
        """
        Load data from CSV files into SQLite database.

        Args:
            force_reload: If True, drop existing tables and reload data
        """
        conn = self.connect()

        # Check if database already populated
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        existing_tables = [row[0] for row in cursor.fetchall()]

        if existing_tables and not force_reload:
            logger.info("Database already populated. Use force_reload=True to reload.")
            return

        logger.info("Loading KPME data from CSV files...")

        # Drop existing tables if force reload
        if force_reload:
            for table in ['establishments', 'staff', 'specialties', 'certificates', 'treatments']:
                cursor.execute(f"DROP TABLE IF EXISTS {table}")
            conn.commit()
            logger.info("Dropped existing tables")

        # Load main establishments
        csv_path = self.csv_dir / "KPME_FULL_DATA.csv"
        if csv_path.exists():
            df = pd.read_csv(csv_path)
            df.to_sql('establishments', conn, if_exists='replace', index=False)
            logger.info(f"✓ Loaded {len(df)} establishments")
        else:
            logger.warning(f"CSV not found: {csv_path}")

        # Load staff
        csv_path = self.csv_dir / "KPME_STAFF.csv"
        if csv_path.exists():
            df = pd.read_csv(csv_path)
            df.to_sql('staff', conn, if_exists='replace', index=False)
            logger.info(f"✓ Loaded {len(df)} staff records")

        # Load specialties
        csv_path = self.csv_dir / "KPME_SPECIALTIES.csv"
        if csv_path.exists():
            df = pd.read_csv(csv_path)
            df.to_sql('specialties', conn, if_exists='replace', index=False)
            logger.info(f"✓ Loaded {len(df)} specialty records")

        # Load certificates
        csv_path = self.csv_dir / "KPME_CERTIFICATES.csv"
        if csv_path.exists():
            df = pd.read_csv(csv_path)
            df.to_sql('certificates', conn, if_exists='replace', index=False)
            logger.info(f"✓ Loaded {len(df)} certificate records")

        # Load treatments
        csv_path = self.csv_dir / "KPME_TREATMENTS.csv"
        if csv_path.exists():
            df = pd.read_csv(csv_path)
            df.to_sql('treatments', conn, if_exists='replace', index=False)
            logger.info(f"✓ Loaded {len(df)} treatment records")

        # Create indexes for faster queries
        self._create_indexes()

        conn.commit()
        logger.info("✅ Database loaded successfully")

    def _create_indexes(self):
        """Create indexes on frequently queried columns."""
        conn = self.connect()
        cursor = conn.cursor()

        indexes = [
            # Establishments indexes
            "CREATE INDEX IF NOT EXISTS idx_est_name ON establishments(establishment_name)",
            "CREATE INDEX IF NOT EXISTS idx_est_cert ON establishments(certificate_number)",
            "CREATE INDEX IF NOT EXISTS idx_est_phone ON establishments(phone)",
            "CREATE INDEX IF NOT EXISTS idx_est_email ON establishments(email)",
            "CREATE INDEX IF NOT EXISTS idx_est_district ON establishments(district)",

            # Staff indexes
            "CREATE INDEX IF NOT EXISTS idx_staff_est ON staff(establishment_id)",
            "CREATE INDEX IF NOT EXISTS idx_staff_reg ON staff(registration_no)",
            "CREATE INDEX IF NOT EXISTS idx_staff_name ON staff(name)",

            # Specialties indexes
            "CREATE INDEX IF NOT EXISTS idx_spec_est ON specialties(establishment_id)",

            # Certificates indexes
            "CREATE INDEX IF NOT EXISTS idx_cert_est ON certificates(establishment_id)",

            # Treatments indexes
            "CREATE INDEX IF NOT EXISTS idx_treat_est ON treatments(establishment_id)"
        ]

        for idx_sql in indexes:
            cursor.execute(idx_sql)

        conn.commit()
        logger.info("✓ Created database indexes")

    def search_establishment_by_name(self, name: str, limit: int = 100) -> List[Dict[str, Any]]:
        """
        Search establishments by name (case-insensitive, partial match).

        Args:
            name: Establishment name to search
            limit: Maximum results to return

        Returns:
            List of matching establishment records
        """
        conn = self.connect()
        cursor = conn.cursor()

        query = """
            SELECT * FROM establishments
            WHERE establishment_name LIKE ?
            ORDER BY establishment_name
            LIMIT ?
        """

        cursor.execute(query, (f"%{name}%", limit))
        return [dict(row) for row in cursor.fetchall()]

    def get_establishment_by_certificate(self, cert_number: str) -> Optional[Dict[str, Any]]:
        """
        Get establishment by certificate number.

        Args:
            cert_number: KPME certificate number

        Returns:
            Establishment record or None
        """
        conn = self.connect()
        cursor = conn.cursor()

        query = "SELECT * FROM establishments WHERE certificate_number = ?"
        cursor.execute(query, (cert_number,))

        row = cursor.fetchone()
        return dict(row) if row else None

    def get_establishment_by_phone(self, phone: str) -> List[Dict[str, Any]]:
        """
        Get establishments by phone number.

        Args:
            phone: Phone number

        Returns:
            List of matching establishments
        """
        conn = self.connect()
        cursor = conn.cursor()

        # Clean phone number (remove spaces, dashes)
        clean_phone = phone.replace(" ", "").replace("-", "").replace("+", "")

        query = """
            SELECT * FROM establishments
            WHERE REPLACE(REPLACE(REPLACE(phone, ' ', ''), '-', ''), '+', '') LIKE ?
        """

        cursor.execute(query, (f"%{clean_phone}%",))
        return [dict(row) for row in cursor.fetchall()]

    def get_establishment_with_staff(self, establishment_id: int) -> Dict[str, Any]:
        """
        Get complete establishment data with staff, specialties, etc.

        Args:
            establishment_id: Establishment ID

        Returns:
            Complete establishment data
        """
        conn = self.connect()
        cursor = conn.cursor()

        # Get establishment
        cursor.execute("SELECT * FROM establishments WHERE id = ?", (establishment_id,))
        est = cursor.fetchone()
        if not est:
            return {}

        result = dict(est)

        # Get staff
        cursor.execute("SELECT * FROM staff WHERE establishment_id = ?", (establishment_id,))
        result['staff'] = [dict(row) for row in cursor.fetchall()]

        # Get specialties
        cursor.execute("SELECT * FROM specialties WHERE establishment_id = ?", (establishment_id,))
        result['specialties'] = [dict(row) for row in cursor.fetchall()]

        # Get certificates
        cursor.execute("SELECT * FROM certificates WHERE establishment_id = ?", (establishment_id,))
        result['certificates'] = [dict(row) for row in cursor.fetchall()]

        # Get treatments
        cursor.execute("SELECT * FROM treatments WHERE establishment_id = ?", (establishment_id,))
        result['treatments'] = [dict(row) for row in cursor.fetchall()]

        return result

    def search_staff_by_name(self, name: str, limit: int = 10) -> List[Dict[str, Any]]:
        """
        Search staff by name.

        Args:
            name: Staff name to search
            limit: Maximum results

        Returns:
            List of staff records with establishment info
        """
        conn = self.connect()
        cursor = conn.cursor()

        query = """
            SELECT s.*, e.establishment_name, e.district, e.address
            FROM staff s
            JOIN establishments e ON s.establishment_id = e.id
            WHERE s.name LIKE ?
            LIMIT ?
        """

        cursor.execute(query, (f"%{name}%", limit))
        return [dict(row) for row in cursor.fetchall()]

    def search_staff_by_registration(self, reg_no: str) -> Optional[Dict[str, Any]]:
        """
        Search staff by registration number.

        Args:
            reg_no: Registration number

        Returns:
            Staff record with establishment info or None
        """
        conn = self.connect()
        cursor = conn.cursor()

        query = """
            SELECT s.*, e.establishment_name, e.district, e.address, e.phone as establishment_phone
            FROM staff s
            JOIN establishments e ON s.establishment_id = e.id
            WHERE s.registration_no = ?
        """

        cursor.execute(query, (reg_no,))
        row = cursor.fetchone()
        return dict(row) if row else None

    def get_establishments_by_district(self, district: str) -> List[Dict[str, Any]]:
        """
        Get all establishments in a district.

        Args:
            district: District name

        Returns:
            List of establishments
        """
        conn = self.connect()
        cursor = conn.cursor()

        query = """
            SELECT * FROM establishments
            WHERE district = ?
            ORDER BY establishment_name
        """

        cursor.execute(query, (district.upper(),))
        return [dict(row) for row in cursor.fetchall()]

    def get_stats(self) -> Dict[str, Any]:
        """
        Get database statistics.

        Returns:
            Dictionary with counts and stats
        """
        conn = self.connect()
        cursor = conn.cursor()

        stats = {}

        # Count establishments
        cursor.execute("SELECT COUNT(*) FROM establishments")
        stats['total_establishments'] = cursor.fetchone()[0]

        # Count staff
        cursor.execute("SELECT COUNT(*) FROM staff")
        stats['total_staff'] = cursor.fetchone()[0]

        # Count total unique districts
        cursor.execute("""
            SELECT COUNT(DISTINCT district)
            FROM establishments
            WHERE district != '' AND district IS NOT NULL
        """)
        stats['total_districts'] = cursor.fetchone()[0]

        # Count by district
        cursor.execute("""
            SELECT district, COUNT(*) as count
            FROM establishments
            WHERE district != ''
            GROUP BY district
            ORDER BY count DESC
            LIMIT 10
        """)
        stats['top_districts'] = [dict(row) for row in cursor.fetchall()]

        # Count by system of medicine
        cursor.execute("""
            SELECT system_of_medicine, COUNT(*) as count
            FROM establishments
            GROUP BY system_of_medicine
            ORDER BY count DESC
        """)
        stats['by_system'] = [dict(row) for row in cursor.fetchall()]

        # Establishments with GPS
        cursor.execute("""
            SELECT COUNT(*) FROM establishments
            WHERE latitude IS NOT NULL AND latitude != ''
        """)
        stats['with_gps'] = cursor.fetchone()[0]

        return stats

    def __enter__(self):
        """Context manager entry."""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()


# Singleton instance
_db_instance: Optional[KPMEDatabase] = None


def get_kpme_db(force_reload: bool = False) -> KPMEDatabase:
    """
    Get singleton KPME database instance.

    Args:
        force_reload: Force reload from CSV files

    Returns:
        KPMEDatabase instance
    """
    global _db_instance

    if _db_instance is None:
        _db_instance = KPMEDatabase()
        _db_instance.load_from_csv(force_reload=force_reload)
    elif force_reload:
        _db_instance.load_from_csv(force_reload=True)

    return _db_instance
