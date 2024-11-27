import streamlit as st
import pandas as pd
import psycopg
import credentials
import matplotlib.pyplot as plt


# Set page configuration to wide mode
st.set_page_config(
    page_title="Hospital Logistics Dashboard",
    page_icon="🏥",
    layout="wide"
)

# make sure dropdown doesn't become too wide
st.markdown(
    """
    <style>
    .stSelectbox {
        width: 200px !important;
    }
    </style>
    """,
    unsafe_allow_html=True,
)


if __name__ == "__main__":
    st.title("Hospital Logistics Dashboard")

    conn = psycopg.connect(
        host="pinniped.postgres.database.azure.com",
        dbname=credentials.DB_USER,
        user=credentials.DB_USER,
        password=credentials.DB_PASSWORD
    )

    # query to get available collection week from data
    q_gen_1 = """SELECT distinct collection_week as week
    FROM HospitalLogistics
    ORDER BY Week DESC"""
    all_weeks_hhs = pd.read_sql_query(q_gen_1, conn)
    print(all_weeks_hhs)

    # Create a dropdown based on unique collection weeks
    # based on the collection week, we will update rest of the report
    st.title("Select Collection Week")

    selected_week = st.selectbox(
        "Choose a collection week:",
        all_weeks_hhs["week"].unique()
    )

    st.write(f"#### You selected: {selected_week}")

    # ------------------------------ Report 1 ---------------------------------
    # print number of records loaded in current and past weeks
    q_rpt_1 = """
    SELECT collection_week as week, count(*) as num_records
    FROM HospitalLogistics
    WHERE collection_week <= %(selected_week)s
    GROUP BY collection_week
    ORDER BY collection_week DESC
    """

    parameters = {'selected_week': selected_week}
    df_rpt_1 = pd.read_sql_query(q_rpt_1, conn, params=parameters)

    # Display Report 1 on dashboard
    st.write("## Records Loaded Across Weeks")
    st.dataframe(df_rpt_1)
    # ------------------------------ Report 1 ---------------------------------

    # ------------------------------ Report 2 ---------------------------------
    # Query execution
    q_rpt_2 = """
    WITH WeeklySummary AS (
        SELECT 
            collection_week,
            SUM(COALESCE(all_adult_hospital_beds_7_day_avg, 0)) AS total_adult_beds,
            SUM(COALESCE(all_pediatric_inpatient_beds_7_day_avg, 0)) AS total_pediatric_beds,
            SUM(COALESCE(all_adult_hospital_inpatient_bed_occupied_7_day_avg, 0)) AS adult_beds_used,
            SUM(COALESCE(all_pediatric_inpatient_bed_occupied_7_day_avg, 0)) AS pediatric_beds_used,
            SUM(COALESCE(inpatient_beds_used_covid_7_day_avg, 0)) AS beds_used_by_covid
        FROM HospitalLogistics
        WHERE collection_week <= %(selected_week)s
        AND collection_week > %(selected_week)s - INTERVAL '4 weeks'
        GROUP BY collection_week
        ORDER BY collection_week DESC
    )
    SELECT 
        collection_week AS Week,
        total_adult_beds AS "Total Adult Beds",
        adult_beds_used AS "Adult Beds Used",
        total_pediatric_beds AS "Total Pediatric Beds",
        pediatric_beds_used AS "Pediatric Beds Used",
        beds_used_by_covid AS "Beds Used by COVID Patients"
    FROM WeeklySummary;
    """

    parameters = {'selected_week': selected_week}
    df_rpt_2 = pd.read_sql_query(q_rpt_2, conn, params=parameters)

    # Display Report 2 on dashboard
    st.write("## Weekly Bed Utilization Summary")
    st.dataframe(df_rpt_2)
    # ------------------------------ Report 2 ---------------------------------

    # ------------------------------ Report 3 ---------------------------------
    q_rpt_3 = """
    WITH BedUsage AS (
        SELECT 
            hq.hospital_pk,
            hq.hospital_overall_rating,
            SUM(hl.all_adult_hospital_inpatient_bed_occupied_7_day_avg) / NULLIF(SUM(hl.all_adult_hospital_beds_7_day_avg), 0) AS adult_bed_usage_fraction,
            SUM(hl.all_pediatric_inpatient_bed_occupied_7_day_avg) / NULLIF(SUM(hl.all_pediatric_inpatient_beds_7_day_avg), 0) AS pediatric_bed_usage_fraction
        FROM 
            HospitalLogistics hl
        JOIN 
            HospitalQualityDetails hq 
            ON hl.hospital_pk = hq.hospital_pk
        WHERE 
            hl.collection_week = %(selected_week)s
        GROUP BY 
            hq.hospital_pk, hq.hospital_overall_rating
    )
    SELECT 
        hospital_overall_rating AS "Quality Rating",
        AVG(adult_bed_usage_fraction) AS "Average Adult Bed Usage",
        AVG(pediatric_bed_usage_fraction) AS "Average Pediatric Bed Usage"
    FROM 
        BedUsage
    GROUP BY
        hospital_overall_rating
    ORDER BY 
        "Quality Rating";
    """

    parameters = {'selected_week': selected_week}
    df_rpt_3 = pd.read_sql_query(q_rpt_3, conn, params=parameters)

    # Display Report 3 on dashboard
    st.write("## Hospital Bed Usage by Quality Rating")

    fig, ax = plt.subplots(figsize=(4, 1.5))

    # line or bar, whatever we want plot
    df_rpt_3.plot(kind="line", x="Quality Rating", y=["Average Adult Bed Usage", "Average Pediatric Bed Usage"], ax=ax)

    plt.title("Average Bed Usage by Hospital Quality Rating", fontsize=4)
    plt.xlabel("Quality Rating", fontsize=4)
    plt.ylabel("Average Bed Usage Fraction", fontsize=4)
    plt.legend(fontsize=3)
    plt.xticks(rotation=0, fontsize=4)
    plt.yticks(fontsize=4)
    plt.tight_layout()

    st.pyplot(fig)
    # ------------------------------ Report 3 ---------------------------------
    
    # ------------------------------ Report 4 ---------------------------------
    
    # ------------------------------ Report 4 ---------------------------------
