"""
Rural Health Statistics (RHS) PDF Parser for Karnataka

Parses PDF tables from RHS reports to extract district-wise PHC/CHC/SC data
for Karnataka. Uses tabula-py for PDF table extraction.
"""

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any, Optional
import pandas as pd

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class RHSParser:
    """Parser for Rural Health Statistics PDF reports"""

    def __init__(
        self,
        pdf_path: str,
        output_dir: str = "dataset/raw/rhs"
    ):
        self.pdf_path = Path(pdf_path)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.parsed_data: List[pd.DataFrame] = []

    def parse_pdf_tables(
        self,
        pages: str = "all",
        multiple_tables: bool = True
    ) -> List[pd.DataFrame]:
        """
        Parse tables from PDF using tabula-py

        Args:
            pages: Page numbers to parse ('all' or '1,2,3' or '1-5')
            multiple_tables: Whether to extract multiple tables per page

        Returns:
            List of DataFrames
        """
        try:
            import tabula
        except ImportError:
            logger.error("tabula-py not installed. Install with: pip install tabula-py")
            logger.info("Note: Also requires Java Runtime Environment")
            return []

        logger.info(f"Parsing PDF: {self.pdf_path}")

        try:
            tables = tabula.read_pdf(
                str(self.pdf_path),
                pages=pages,
                multiple_tables=multiple_tables,
                pandas_options={'header': 0}
            )

            logger.info(f"Extracted {len(tables)} tables")
            self.parsed_data = tables
            return tables

        except Exception as e:
            logger.error(f"Error parsing PDF: {e}")
            return []

    def find_karnataka_table(
        self,
        tables: List[pd.DataFrame]
    ) -> Optional[pd.DataFrame]:
        """
        Find the table containing Karnataka data

        Args:
            tables: List of DataFrames

        Returns:
            DataFrame with Karnataka data or None
        """
        for i, table in enumerate(tables):
            # Check if any column contains "Karnataka"
            for col in table.columns:
                if table[col].astype(str).str.contains('Karnataka', case=False).any():
                    logger.info(f"Found Karnataka data in table {i}")
                    return table

            # Check if any row contains "Karnataka"
            for idx, row in table.iterrows():
                if any('karnataka' in str(val).lower() for val in row.values):
                    logger.info(f"Found Karnataka data in table {i}")
                    return table

        logger.warning("Karnataka table not found")
        return None

    def extract_karnataka_districts(
        self,
        df: pd.DataFrame
    ) -> pd.DataFrame:
        """
        Extract Karnataka district-wise data

        Args:
            df: DataFrame with Karnataka data

        Returns:
            Filtered and cleaned DataFrame
        """
        logger.info("Extracting Karnataka districts...")

        # Find Karnataka section in the table
        karnataka_rows = []
        in_karnataka_section = False

        for idx, row in df.iterrows():
            row_str = ' '.join(row.astype(str).values).lower()

            # Start of Karnataka section
            if 'karnataka' in row_str:
                in_karnataka_section = True
                continue

            # End of Karnataka section (next state or total)
            if in_karnataka_section and any(keyword in row_str for keyword in ['total', 'all india', 'kerala', 'goa']):
                break

            # Collect Karnataka district rows
            if in_karnataka_section:
                karnataka_rows.append(row)

        if karnataka_rows:
            karnataka_df = pd.DataFrame(karnataka_rows)
            logger.info(f"Extracted {len(karnataka_df)} district records")
            return karnataka_df
        else:
            logger.warning("No Karnataka district data found")
            return pd.DataFrame()

    def standardize_column_names(self, df: pd.DataFrame) -> pd.DataFrame:
        """Standardize column names"""
        # Common RHS column mappings
        column_mappings = {
            'district': 'district',
            'sc': 'sub_centres',
            'phc': 'phc',
            'chc': 'chc',
            'sdh': 'sub_divisional_hospital',
            'dh': 'district_hospital',
            'beds': 'beds',
            'doctors': 'doctors',
            'nurses': 'nurses'
        }

        new_columns = {}
        for col in df.columns:
            col_lower = str(col).lower().strip()
            for key, value in column_mappings.items():
                if key in col_lower:
                    new_columns[col] = value
                    break

        df = df.rename(columns=new_columns)
        return df

    def parse_karnataka_rhs(
        self,
        pages: str = "all"
    ) -> pd.DataFrame:
        """
        Complete pipeline to parse Karnataka RHS data

        Args:
            pages: Pages to parse

        Returns:
            Cleaned Karnataka district data
        """
        # Parse PDF
        tables = self.parse_pdf_tables(pages)

        if not tables:
            return pd.DataFrame()

        # Find Karnataka table
        karnataka_table = self.find_karnataka_table(tables)

        if karnataka_table is None:
            return pd.DataFrame()

        # Extract districts
        districts_df = self.extract_karnataka_districts(karnataka_table)

        if districts_df.empty:
            return pd.DataFrame()

        # Standardize columns
        districts_df = self.standardize_column_names(districts_df)

        # Clean data
        districts_df = districts_df.apply(pd.to_numeric, errors='ignore')

        return districts_df

    def save_parsed_data(
        self,
        df: pd.DataFrame,
        filename: str = "karnataka_rhs_parsed.csv"
    ):
        """Save parsed RHS data"""
        if df.empty:
            logger.warning("No data to save")
            return

        # Save CSV
        csv_path = self.output_dir / filename
        df.to_csv(csv_path, index=False)
        logger.info(f"Saved {len(df)} records to {csv_path}")

        # Save JSON
        json_path = self.output_dir / filename.replace('.csv', '.json')
        data = {
            "parsed_at": datetime.now().isoformat(),
            "source": f"Rural Health Statistics - {self.pdf_path.name}",
            "state": "Karnataka",
            "total_districts": len(df),
            "data": df.to_dict(orient="records")
        }

        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)

        logger.info(f"Saved JSON to {json_path}")

    def save_all_tables(self, tables: List[pd.DataFrame]):
        """Save all extracted tables for manual review"""
        for i, table in enumerate(tables):
            filename = f"rhs_table_{i+1}.csv"
            filepath = self.output_dir / filename
            table.to_csv(filepath, index=False)
            logger.info(f"Saved table {i+1} to {filepath}")


def main():
    """Main execution function"""
    import sys

    if len(sys.argv) < 2:
        print("Usage: python rhs_parser.py <path_to_rhs_pdf>")
        print("\nExample:")
        print("  python rhs_parser.py dataset/raw/rhs/RHS_2023.pdf")
        return

    pdf_path = sys.argv[1]

    if not Path(pdf_path).exists():
        logger.error(f"PDF file not found: {pdf_path}")
        return

    logger.info("Starting RHS PDF parser for Karnataka")

    parser = RHSParser(pdf_path)

    # Parse Karnataka data
    karnataka_df = parser.parse_karnataka_rhs()

    # Save parsed data
    if not karnataka_df.empty:
        parser.save_parsed_data(karnataka_df)

        # Also save all tables for manual review
        if parser.parsed_data:
            parser.save_all_tables(parser.parsed_data)

        logger.info("RHS parsing completed successfully")
    else:
        logger.warning("No Karnataka data extracted. Check PDF manually.")


if __name__ == "__main__":
    main()
