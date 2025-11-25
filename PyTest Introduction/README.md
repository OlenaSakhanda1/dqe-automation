# PyTest Introduction

This project demonstrates how to use **PyTest** for testing CSV data files with schema validation and generating HTML reports.

---

## 📂 Project Structure
PyTest Introduction/
│
├── requirements.txt       # Project dependencies
├── README.md              # Documentation
├── src/
│   └── data/
│       └── data.csv       # Sample data file
└── tests/
├── conftest.py        # PyTest fixtures
├── pytest.ini         # PyTest configuration
└── test_csv/
└── test_csv_file.py  # Test cases

## ✅ Installation

1. **Clone the repository**:
   ```bash
   git clone [<your-repo-url>](https://github.com/OlenaSakhanda1/dqe-automation/tree/main)
   cd PyTest Introduction
2. **Install dependencies**:
   pip install -r requirements.txt
3. **Running Tests**:
   pytest
   If you want to generate an HTML report (optional), install pytest-html and run:
   pytest --html=report.html --self-contained-html

   
