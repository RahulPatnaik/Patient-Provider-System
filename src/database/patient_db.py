"""
Patient Records Database Manager
Handles patient medical records with SQLite storage
"""

import sqlite3
import logging
from datetime import datetime
from typing import Dict, List, Optional, Any
from pathlib import Path

logger = logging.getLogger(__name__)


class PatientDatabase:
    """Patient records database manager with full CRUD operations"""

    def __init__(self, db_path: str = "src/database/patients.db"):
        self.db_path = db_path
        self._ensure_db_exists()
        logger.info(f"Patient Database initialized: {self.db_path}")

    def _ensure_db_exists(self):
        """Create database and tables if they don't exist"""
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)

        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        # Create patients table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS patients (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                patient_name TEXT NOT NULL,
                date_of_birth TEXT,
                gender TEXT,
                phone TEXT,
                email TEXT,
                address TEXT,
                emergency_contact_name TEXT,
                emergency_contact_phone TEXT,
                blood_group TEXT,
                allergies TEXT,
                chronic_conditions TEXT,
                medical_notes TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
        """)

        # Create prescription_records table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS prescription_records (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                patient_id INTEGER NOT NULL,
                prescription_date TEXT NOT NULL,
                doctor_name TEXT NOT NULL,
                doctor_license TEXT,
                clinic_name TEXT NOT NULL,
                clinic_location TEXT,
                clinic_phone TEXT,
                diagnosis TEXT,
                medications TEXT NOT NULL,
                dosage_instructions TEXT,
                notes TEXT,
                prescription_image_path TEXT,
                extracted_text TEXT,
                created_at TEXT NOT NULL,
                FOREIGN KEY (patient_id) REFERENCES patients(id) ON DELETE CASCADE
            )
        """)

        # Create indexes for fast search
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_patient_name ON patients(patient_name)
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_patient_phone ON patients(phone)
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_patient_email ON patients(email)
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_prescription_patient ON prescription_records(patient_id)
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_prescription_date ON prescription_records(prescription_date)
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_prescription_doctor ON prescription_records(doctor_name)
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_prescription_clinic ON prescription_records(clinic_name)
        """)

        conn.commit()
        conn.close()

        logger.info("Patient database tables and indexes created/verified")

    def _get_connection(self):
        """Get database connection with row factory"""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    # ==================== PATIENT CRUD ====================

    def create_patient(self, patient_data: Dict[str, Any]) -> int:
        """
        Create a new patient record

        Args:
            patient_data: Dictionary with patient information

        Returns:
            Patient ID
        """
        conn = self._get_connection()
        cursor = conn.cursor()

        now = datetime.now().isoformat()

        cursor.execute("""
            INSERT INTO patients (
                patient_name, date_of_birth, gender, phone, email, address,
                emergency_contact_name, emergency_contact_phone,
                blood_group, allergies, chronic_conditions,
                created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            patient_data.get('patient_name'),
            patient_data.get('date_of_birth'),
            patient_data.get('gender'),
            patient_data.get('phone'),
            patient_data.get('email'),
            patient_data.get('address'),
            patient_data.get('emergency_contact_name'),
            patient_data.get('emergency_contact_phone'),
            patient_data.get('blood_group'),
            patient_data.get('allergies'),
            patient_data.get('chronic_conditions'),
            now,
            now
        ))

        patient_id = cursor.lastrowid
        conn.commit()
        conn.close()

        logger.info(f"Created patient record: ID={patient_id}, Name={patient_data.get('patient_name')}")
        return patient_id

    def get_patient(self, patient_id: int) -> Optional[Dict[str, Any]]:
        """Get patient by ID"""
        conn = self._get_connection()
        cursor = conn.cursor()

        cursor.execute("SELECT * FROM patients WHERE id = ?", (patient_id,))
        row = cursor.fetchone()
        conn.close()

        if row:
            return dict(row)
        return None

    def update_patient(self, patient_id: int, patient_data: Dict[str, Any]) -> bool:
        """Update patient record"""
        conn = self._get_connection()
        cursor = conn.cursor()

        now = datetime.now().isoformat()

        cursor.execute("""
            UPDATE patients SET
                patient_name = ?,
                date_of_birth = ?,
                gender = ?,
                phone = ?,
                email = ?,
                address = ?,
                emergency_contact_name = ?,
                emergency_contact_phone = ?,
                blood_group = ?,
                allergies = ?,
                chronic_conditions = ?,
                updated_at = ?
            WHERE id = ?
        """, (
            patient_data.get('patient_name'),
            patient_data.get('date_of_birth'),
            patient_data.get('gender'),
            patient_data.get('phone'),
            patient_data.get('email'),
            patient_data.get('address'),
            patient_data.get('emergency_contact_name'),
            patient_data.get('emergency_contact_phone'),
            patient_data.get('blood_group'),
            patient_data.get('allergies'),
            patient_data.get('chronic_conditions'),
            now,
            patient_id
        ))

        rows_updated = cursor.rowcount
        conn.commit()
        conn.close()

        logger.info(f"Updated patient record: ID={patient_id}")
        return rows_updated > 0

    def delete_patient(self, patient_id: int) -> bool:
        """Delete patient and all associated prescriptions"""
        conn = self._get_connection()
        cursor = conn.cursor()

        cursor.execute("DELETE FROM patients WHERE id = ?", (patient_id,))
        rows_deleted = cursor.rowcount

        conn.commit()
        conn.close()

        logger.info(f"Deleted patient record: ID={patient_id}")
        return rows_deleted > 0

    def search_patients(
        self,
        query: str = None,
        phone: str = None,
        email: str = None,
        limit: int = 50
    ) -> List[Dict[str, Any]]:
        """
        Search patients by name, phone, or email

        Args:
            query: Name search (fuzzy)
            phone: Exact phone match
            email: Exact email match
            limit: Maximum results

        Returns:
            List of patient records
        """
        conn = self._get_connection()
        cursor = conn.cursor()

        conditions = []
        params = []

        if query:
            conditions.append("patient_name LIKE ?")
            params.append(f"%{query}%")

        if phone:
            conditions.append("phone = ?")
            params.append(phone)

        if email:
            conditions.append("email LIKE ?")
            params.append(f"%{email}%")

        if not conditions:
            # Return all patients if no filter
            sql = f"SELECT * FROM patients ORDER BY created_at DESC LIMIT ?"
            params = [limit]
        else:
            where_clause = " OR ".join(conditions)
            sql = f"SELECT * FROM patients WHERE {where_clause} ORDER BY created_at DESC LIMIT ?"
            params.append(limit)

        cursor.execute(sql, params)
        rows = cursor.fetchall()
        conn.close()

        results = [dict(row) for row in rows]
        logger.info(f"Patient search: query='{query}', found {len(results)} results")
        return results

    # ==================== PRESCRIPTION CRUD ====================

    def add_prescription(self, prescription_data: Dict[str, Any]) -> int:
        """
        Add prescription record for a patient

        Args:
            prescription_data: Dictionary with prescription information

        Returns:
            Prescription ID
        """
        conn = self._get_connection()
        cursor = conn.cursor()

        now = datetime.now().isoformat()

        cursor.execute("""
            INSERT INTO prescription_records (
                patient_id, prescription_date, doctor_name, doctor_license,
                clinic_name, clinic_location, clinic_phone,
                diagnosis, medications, dosage_instructions, notes,
                prescription_image_path, extracted_text, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            prescription_data.get('patient_id'),
            prescription_data.get('prescription_date'),
            prescription_data.get('doctor_name'),
            prescription_data.get('doctor_license'),
            prescription_data.get('clinic_name'),
            prescription_data.get('clinic_location'),
            prescription_data.get('clinic_phone'),
            prescription_data.get('diagnosis'),
            prescription_data.get('medications'),
            prescription_data.get('dosage_instructions'),
            prescription_data.get('notes'),
            prescription_data.get('prescription_image_path'),
            prescription_data.get('extracted_text'),
            now
        ))

        prescription_id = cursor.lastrowid
        conn.commit()
        conn.close()

        logger.info(
            f"Added prescription: ID={prescription_id}, "
            f"Patient={prescription_data.get('patient_id')}, "
            f"Doctor={prescription_data.get('doctor_name')}"
        )
        return prescription_id

    def get_patient_prescriptions(
        self,
        patient_id: int,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """Get all prescriptions for a patient"""
        conn = self._get_connection()
        cursor = conn.cursor()

        cursor.execute("""
            SELECT * FROM prescription_records
            WHERE patient_id = ?
            ORDER BY prescription_date DESC
            LIMIT ?
        """, (patient_id, limit))

        rows = cursor.fetchall()
        conn.close()

        results = [dict(row) for row in rows]
        logger.info(f"Retrieved {len(results)} prescriptions for patient {patient_id}")
        return results

    def get_prescription(self, prescription_id: int) -> Optional[Dict[str, Any]]:
        """Get prescription by ID"""
        conn = self._get_connection()
        cursor = conn.cursor()

        cursor.execute("SELECT * FROM prescription_records WHERE id = ?", (prescription_id,))
        row = cursor.fetchone()
        conn.close()

        if row:
            return dict(row)
        return None

    def update_prescription(self, prescription_id: int, prescription_data: Dict[str, Any]) -> bool:
        """Update prescription record"""
        conn = self._get_connection()
        cursor = conn.cursor()

        cursor.execute("""
            UPDATE prescription_records SET
                prescription_date = ?,
                doctor_name = ?,
                doctor_license = ?,
                clinic_name = ?,
                clinic_location = ?,
                clinic_phone = ?,
                diagnosis = ?,
                medications = ?,
                dosage_instructions = ?,
                notes = ?
            WHERE id = ?
        """, (
            prescription_data.get('prescription_date'),
            prescription_data.get('doctor_name'),
            prescription_data.get('doctor_license'),
            prescription_data.get('clinic_name'),
            prescription_data.get('clinic_location'),
            prescription_data.get('clinic_phone'),
            prescription_data.get('diagnosis'),
            prescription_data.get('medications'),
            prescription_data.get('dosage_instructions'),
            prescription_data.get('notes'),
            prescription_id
        ))

        rows_updated = cursor.rowcount
        conn.commit()
        conn.close()

        logger.info(f"Updated prescription: ID={prescription_id}")
        return rows_updated > 0

    def delete_prescription(self, prescription_id: int) -> bool:
        """Delete prescription record"""
        conn = self._get_connection()
        cursor = conn.cursor()

        cursor.execute("DELETE FROM prescription_records WHERE id = ?", (prescription_id,))
        rows_deleted = cursor.rowcount

        conn.commit()
        conn.close()

        logger.info(f"Deleted prescription: ID={prescription_id}")
        return rows_deleted > 0

    def search_prescriptions(
        self,
        doctor_name: str = None,
        clinic_name: str = None,
        clinic_location: str = None,
        date_from: str = None,
        date_to: str = None,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """
        Search prescriptions by doctor, clinic, or date range

        Args:
            doctor_name: Doctor name (fuzzy)
            clinic_name: Clinic name (fuzzy)
            clinic_location: Clinic location (fuzzy)
            date_from: Start date (YYYY-MM-DD)
            date_to: End date (YYYY-MM-DD)
            limit: Maximum results

        Returns:
            List of prescription records with patient info
        """
        conn = self._get_connection()
        cursor = conn.cursor()

        conditions = []
        params = []

        if doctor_name:
            conditions.append("pr.doctor_name LIKE ?")
            params.append(f"%{doctor_name}%")

        if clinic_name:
            conditions.append("pr.clinic_name LIKE ?")
            params.append(f"%{clinic_name}%")

        if clinic_location:
            conditions.append("pr.clinic_location LIKE ?")
            params.append(f"%{clinic_location}%")

        if date_from:
            conditions.append("pr.prescription_date >= ?")
            params.append(date_from)

        if date_to:
            conditions.append("pr.prescription_date <= ?")
            params.append(date_to)

        if not conditions:
            sql = """
                SELECT pr.*, p.patient_name
                FROM prescription_records pr
                JOIN patients p ON pr.patient_id = p.id
                ORDER BY pr.prescription_date DESC
                LIMIT ?
            """
            params = [limit]
        else:
            where_clause = " AND ".join(conditions)
            sql = f"""
                SELECT pr.*, p.patient_name
                FROM prescription_records pr
                JOIN patients p ON pr.patient_id = p.id
                WHERE {where_clause}
                ORDER BY pr.prescription_date DESC
                LIMIT ?
            """
            params.append(limit)

        cursor.execute(sql, params)
        rows = cursor.fetchall()
        conn.close()

        results = [dict(row) for row in rows]
        logger.info(f"Prescription search: found {len(results)} results")
        return results

    def get_stats(self) -> Dict[str, Any]:
        """Get database statistics"""
        conn = self._get_connection()
        cursor = conn.cursor()

        cursor.execute("SELECT COUNT(*) FROM patients")
        total_patients = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM prescription_records")
        total_prescriptions = cursor.fetchone()[0]

        cursor.execute("""
            SELECT COUNT(DISTINCT doctor_name)
            FROM prescription_records
            WHERE doctor_name IS NOT NULL AND doctor_name != ''
        """)
        total_doctors = cursor.fetchone()[0]

        cursor.execute("""
            SELECT COUNT(DISTINCT clinic_name)
            FROM prescription_records
            WHERE clinic_name IS NOT NULL AND clinic_name != ''
        """)
        total_clinics = cursor.fetchone()[0]

        # Recent activity
        cursor.execute("""
            SELECT COUNT(*)
            FROM prescription_records
            WHERE created_at >= date('now', '-30 days')
        """)
        prescriptions_last_30_days = cursor.fetchone()[0]

        conn.close()

        return {
            "total_patients": total_patients,
            "total_prescriptions": total_prescriptions,
            "total_doctors": total_doctors,
            "total_clinics": total_clinics,
            "prescriptions_last_30_days": prescriptions_last_30_days
        }


# Singleton instance
_patient_db = None


def get_patient_db() -> PatientDatabase:
    """Get or create patient database singleton"""
    global _patient_db
    if _patient_db is None:
        _patient_db = PatientDatabase()
    return _patient_db
