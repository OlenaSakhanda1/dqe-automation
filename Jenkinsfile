pipeline {
    agent any

    stages {
        stage('Checkout') {
            steps {
                git branch: 'main', url: 'https://github.com/OlenaSakhanda1/dqe-automation.git'
            }
        }

        stage('Install Dependencies') {
            steps {
                sh '''#!/bin/bash
set -e
echo "📂 Current directory:"
pwd
ls -la
echo "📂 Checking PyTest DQ Framework folder:"
ls -la "PyTest DQ Framework"

# Create virtual environment
python3 -m venv venv
source venv/bin/activate

# Install dependencies
pip install --upgrade pip
pip install -r "./PyTest DQ Framework/requirements.txt"
'''
            }
        }

        stage('Generate Parquet Files') {
            steps {
                withCredentials([usernamePassword(credentialsId: 'jenkins-postgres-credentials', usernameVariable: 'POSTGRES_SECRET_USR', passwordVariable: 'POSTGRES_SECRET_PSW')]) {
                    sh '''#!/bin/bash
set -e
source venv/bin/activate

# Create output folder
mkdir -p parquet_output

# Export DB credentials
export POSTGRES_SECRET_USR=$POSTGRES_SECRET_USR
export POSTGRES_SECRET_PSW=$POSTGRES_SECRET_PSW

# Run parquet generation
python3 "./PyTest DQ Framework/generate_parquet.py"

echo "✅ Parquet generation completed. Listing files:"
ls -la parquet_output || echo "No parquet files found"
'''
                }
            }
        }

        stage('Run Pytest') {
            steps {
                sh '''#!/bin/bash
set -e
source venv/bin/activate

# Create report folder
mkdir -p html_report

# Run tests
pytest "./PyTest DQ Framework/tests" -m "parquet_data" \
    --db_host="postgres" \
    --db_port="5432" \
    --db_name="mydatabase" \
    --db_user=$POSTGRES_SECRET_USR \
    --db_password=$POSTGRES_SECRET_PSW \
    --html=html_report/report.html
'''
            }
        }

        stage('Archive Artifacts') {
            steps {
                archiveArtifacts artifacts: 'html_report/**', allowEmptyArchive: true
                archiveArtifacts artifacts: 'parquet_output/**/*.parquet', allowEmptyArchive: true
                publishHTML(target: [
                    allowMissing: false,
                    keepAll: true,
                    reportDir: 'html_report',
                    reportFiles: 'report.html',
                    reportName: 'HTML Test Report'
                ])
            }
        }
    }
}
