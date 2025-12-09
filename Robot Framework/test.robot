*** Settings ***
Library           SeleniumLibrary
Library           ${CURDIR}/helper.py
Library           Collections
Suite Setup       Open Browser    ${REPORT_FILE}    Chrome
Suite Teardown    Close Browser

*** Variables ***
${REPORT_FILE}      file://${CURDIR}/data/report.html
${PARQUET_FOLDER}   ${CURDIR}/data/parquet/
${FILTER_DATE}      2025-11
${TABLE_LOCATOR}    id:f2be9861-78e2-4f00-aa5f-9778ee33830a
${HTML_DATE_COL}    Visit Date
${PARQUET_DATE_COL}    visit_date

*** Test Cases ***
Validate Plotly Table Against Parquet (Selected Dates)
    [Documentation]   Read Plotly table via JS, normalize, filter selected dates, compare with Parquet

    Wait Until Page Contains Element    ${TABLE_LOCATOR}    10s
    ${TABLE_DATA}=    Evaluate    helper.extract_table_by_locator("${TABLE_LOCATOR}")    modules=helper
    ${TARGET_DATES}=  Create List    2025-11-20    2025-11-21    2025-11-22    2025-11-23    2025-11-24    2025-11-25    2025-11-26

    ${RESULT}    ${DIFF}=    Compare Plotly Table With Parquet    ${TABLE_DATA}    ${PARQUET_FOLDER}    ${FILTER_DATE}    ${HTML_DATE_COL}    ${PARQUET_DATE_COL}    ${TARGET_DATES}

    Run Keyword If    ${RESULT}       Log     ✅ DataFrames match for selected dates!
    Run Keyword If    not ${RESULT}   Fail    ❌ Differences for selected dates: ${DIFF}

Validate All Parquet Rows Are In Table
    [Documentation]   Перевіряє, що всі дані з parquet є у таблиці (HTML)
    Wait Until Page Contains Element    ${TABLE_LOCATOR}    10s
    ${TABLE_DATA}=    Evaluate    helper.extract_table_by_locator("${TABLE_LOCATOR}")    modules=helper
    ${RESULT}    ${MISSING}=    Assert All Parquet In Table    ${TABLE_DATA}    ${PARQUET_FOLDER}    ${FILTER_DATE}    ${HTML_DATE_COL}    ${PARQUET_DATE_COL}
    Run Keyword If    ${RESULT}       Log     ✅ All parquet rows are present in the table!
    Run Keyword If    not ${RESULT}   Fail    ❌ Missing rows from table: ${MISSING}