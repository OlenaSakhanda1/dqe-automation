
*** Settings ***
Library           SeleniumLibrary
Library           ${CURDIR}/../libraries/helper.py
Library           Collections
Suite Setup       Open Browser    ${REPORT_FILE}    Chrome
Suite Teardown    Close Browser

*** Variables ***
${REPORT_FILE}      file://${CURDIR}/data/report.html
${PARQUET_FOLDER}   ${CURDIR}/data/parquet/
${FILTER_DATE}      2025-11


*** Test Cases ***
Validate SVG Table Against Parquet (Compare Only Selected Dates)
    [Documentation]   We compare data from SVG and Parquet only on selected dates
    ${TARGET_DATES}=    Create List    2025-11-23    2025-11-24    2025-11-25    2025-11-26
    Sleep    1s
    Wait Until Element Is Visible    xpath=//*[local-name()='text' and contains(@class,'cell-text')]    10s
    ${cells}=    Get WebElements    xpath=//*[local-name()='text' and contains(@class,'cell-text')]
    ${values}=   Create List
    FOR    ${cell}    IN    @{cells}
        ${text}=    Get Text    ${cell}
        Append To List    ${values}    ${text}
    END

    ${colnames}=    Create List    facility_type    visit_date    avg_time_spent
    ${df_html}=     Convert Svg Columns To Dataframe    ${values}    column_names=${colnames}

    ${df_parquet}=   Read Parquet    ${PARQUET_FOLDER}    ${FILTER_DATE}

    # Filter both DataFrames by selected dates
    ${df_html}=      Filter Dataframe By Dates    ${df_html}    visit_date    ${TARGET_DATES}
    ${df_parquet}=   Filter Dataframe By Dates    ${df_parquet}    visit_date    ${TARGET_DATES}

    ${result}    ${differences}=    Compare Dataframes On Date    ${df_html}    ${df_parquet}    date_column=visit_date

    Run Keyword If    ${result} == True     Log    ✅ DataFrames match for selected dates!
    Run Keyword If    ${result} == False    Fail   ❌ Differences for selected dates: ${differences}
