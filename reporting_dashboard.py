import streamlit as st
import pandas as pd
import psycopg
import credentials
import matplotlib.pyplot as plt
from datetime import timedelta


# Set page configuration to wide mode
st.set_page_config(
    page_title="Hospital Logistics Dashboard",
    layout="wide"
)


# Custom CSS to adjust dropdown width
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


def main():
    st.title("Hospital Logistics Dashboard")

    # Establish database connection
    conn = psycopg.connect(
        host="pinniped.postgres.database.azure.com",
        dbname=credentials.DB_USER,
        user=credentials.DB_USER,
        password=credentials.DB_PASSWORD
    )

    # Query to get available collection weeks
    q_gen_1 = """SELECT distinct collection_week as week
    FROM HospitalLogistics
    ORDER BY Week DESC"""
    all_weeks_hhs = pd.read_sql_query(q_gen_1, conn)

    # Create a dropdown for week selection
    st.title("Select Collection Week")
    selected_week = st.selectbox(
        "Choose a collection week:",
        all_weeks_hhs["week"].unique()
    )

    st.write(f"#### You selected: {selected_week}")

    # Create tabs for different reports
    tab1, tab2, tab3, tab4, tab5, tab6, tab7 = st.tabs([
        "Records Loaded",
        "Bed Utilization Summary",
        "Bed Usage by Quality Rating",
        "Total Beds Used",
        "State COVID Cases",
        "Hospital COVID Cases",
        "Non-Reporting Hospitals"
    ])

    with tab1:
        # Report 1: Records Loaded
        q_rpt_1 = """
        SELECT collection_week as week, count(*) as num_records
        FROM HospitalLogistics
        WHERE collection_week <= %(selected_week)s
        GROUP BY collection_week
        ORDER BY collection_week DESC
        """
        parameters = {'selected_week': selected_week}
        df_rpt_1 = pd.read_sql_query(q_rpt_1, conn, params=parameters)
        st.write("## Records Loaded Across Weeks")
        st.dataframe(df_rpt_1)

    with tab2:
        # Report 2: Weekly Bed Utilization Summary
        q_rpt_2 = """
        WITH WeeklySummary AS (
            SELECT
                collection_week,
                SUM(COALESCE(all_adult_hospital_beds_7_day_avg, 0))
                AS total_adult_beds,
                SUM(COALESCE(all_pediatric_inpatient_beds_7_day_avg, 0))
                AS total_pediatric_beds,
                SUM(
                COALESCE(
                    all_adult_hospital_inpatient_bed_occupied_7_day_avg,
                    0
                )
                ) AS adult_beds_used,
                SUM(
                COALESCE(all_pediatric_inpatient_bed_occupied_7_day_avg, 0))
                AS pediatric_beds_used,
                SUM(COALESCE(inpatient_beds_used_covid_7_day_avg, 0))
                AS beds_used_by_covid
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
        st.write("## Weekly Bed Utilization Summary")
        st.dataframe(df_rpt_2)

    with tab3:
        # Report 3: Hospital Bed Usage by Quality Rating
        q_rpt_3 = """
        WITH BedUsage AS (
            SELECT
                hq.hospital_pk,
                hq.hospital_overall_rating,
                SUM(hl.all_adult_hospital_inpatient_bed_occupied_7_day_avg)/
                NULLIF(SUM(hl.all_adult_hospital_beds_7_day_avg), 0)
                AS adult_bed_usage_fraction,
                SUM(hl.all_pediatric_inpatient_bed_occupied_7_day_avg)/
                NULLIF(SUM(hl.all_pediatric_inpatient_beds_7_day_avg), 0)
                AS pediatric_bed_usage_fraction
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

        fig, ax = plt.subplots(figsize=(10, 5))
        df_rpt_3.plot(kind="line", x="Quality Rating",
                      y=["Average Adult Bed Usage",
                         "Average Pediatric Bed Usage"],
                      ax=ax)

        plt.title("Average Bed Usage by Hospital Quality Rating")
        plt.xlabel("Quality Rating")
        plt.ylabel("Average Bed Usage Fraction")
        plt.legend()
        plt.tight_layout()

        st.write("## Hospital Bed Usage by Quality Rating")
        st.pyplot(fig)

    with tab4:
        # Report 4: Total Hospital Beds Used Per Week
        q_rpt_4 = """
        WITH BedUsage AS (
            SELECT
                collection_week,
                SUM(all_adult_hospital_inpatient_bed_occupied_7_day_avg) +
                SUM(all_pediatric_inpatient_bed_occupied_7_day_avg)
                    AS total_beds_used,
                SUM(inpatient_beds_used_covid_7_day_avg) AS covid_beds_used
            FROM HospitalLogistics
            WHERE collection_week <= %(selected_week)s
            GROUP BY collection_week
            ORDER BY collection_week
        )
        SELECT
            collection_week AS "Week",
            total_beds_used AS "Total Beds Usage",
            covid_beds_used AS "COVID Beds Usage",
            (total_beds_used - covid_beds_used) AS "Non-COVID Beds Usage"
        FROM BedUsage
        ORDER BY "Week";
        """
        parameters = {'selected_week': selected_week}
        df_rpt_4 = pd.read_sql_query(q_rpt_4, conn, params=parameters)

        fig, ax = plt.subplots(figsize=(12, 5))
        ax.plot(df_rpt_4['Week'], df_rpt_4['Total Beds Usage'],
                label='Total Beds Used', color='blue')
        ax.plot(df_rpt_4['Week'], df_rpt_4['COVID Beds Usage'],
                label='COVID Beds Used', color='green')

        plt.title('Hospital Beds Usage per Week')
        plt.xlabel('Week')
        plt.ylabel('Number of Beds Used')
        plt.legend()
        plt.xticks(rotation=45)
        plt.tight_layout()

        st.write("## Total Hospital Beds Used Per Week: COVID vs Non-COVID")
        st.pyplot(fig)

    with tab5:
        # Report 5: States with Largest Increase in COVID Cases
        q_rpt_5 = """
        WITH WeeklyCases AS (
        SELECT
            SUBSTRING(CAST(fips_code AS TEXT), 1, 2) AS state,
            collection_week,
            SUM(inpatient_beds_used_covid_7_day_avg) AS covid_beds
        FROM HospitalLogistics
        JOIN HospitalSpecificDetails
        ON HospitalSpecificDetails.hospital_pk = HospitalLogistics.hospital_pk
        WHERE collection_week IN (%(selected_week)s, %(previous_week)s)
        GROUP BY state, fips_code, collection_week
        ),
        ChangeInCases AS (
            SELECT
                current.state,
                current.covid_beds AS covid_beds_this_week,
                COALESCE(previous.covid_beds, 0) AS covid_beds_last_week,
                (current.covid_beds - COALESCE(previous.covid_beds, 0))
                    AS increase_in_cases
            FROM
                (
                SELECT *
                FROM WeeklyCases
                WHERE collection_week = %(selected_week)s) AS current
            LEFT JOIN
                (SELECT *
                FROM WeeklyCases
                WHERE collection_week = %(previous_week)s) AS previous
            ON current.state = previous.state
        )
        SELECT
            state AS "State",
            covid_beds_this_week AS "COVID Cases This Week",
            covid_beds_last_week AS "COVID Cases Last Week",
            increase_in_cases AS "Increase In COVID Cases"
        FROM ChangeInCases
        WHERE state IS NOT NULL AND
        covid_beds_this_week IS NOT NULL AND
        covid_beds_last_week IS NOT NULL AND
        increase_in_cases IS NOT NULL AND
        covid_beds_last_week != 0
        ORDER BY increase_in_cases DESC;
        """
        parameters = {'selected_week': selected_week,
                      'previous_week': selected_week - timedelta(weeks=1)
                      }

        df_rpt_5 = pd.read_sql_query(q_rpt_5, conn, params=parameters)
        df_rpt_5.index = df_rpt_5.index + 1

        st.write("## 10 States with Largest Increase in COVID Cases")
        st.dataframe(df_rpt_5.head(10), use_container_width=True)

    with tab6:
        # Report 6: Hospitals with Biggest Weekly Difference in COVID Cases
        q_rpt_6 = """
        WITH WeeklyCases AS (
            SELECT
                hospital_name,
                city,
                collection_week,
                SUM(inpatient_beds_used_covid_7_day_avg) AS covid_beds
            FROM HospitalLogistics
            JOIN HospitalSpecificDetails
            ON
            HospitalSpecificDetails.hospital_pk = HospitalLogistics.hospital_pk
            WHERE collection_week IN (%(selected_week)s, %(previous_week)s)
            GROUP BY hospital_name, city, collection_week
        ),
        ChangeInCases AS (
            SELECT
                current.hospital_name,
                current.city,
                current.covid_beds AS covid_beds_this_week,
                COALESCE(previous.covid_beds, 0) AS covid_beds_last_week,
                ABS(current.covid_beds - COALESCE(previous.covid_beds, 0))
                    AS cases_difference
            FROM
                (
                SELECT *
                FROM WeeklyCases
                WHERE collection_week = %(selected_week)s) current
            LEFT JOIN
                (
                SELECT *
                FROM WeeklyCases
                WHERE collection_week = %(previous_week)s) previous
            ON current.hospital_name = previous.hospital_name
            AND current.city = previous.city
        )
        SELECT
            hospital_name AS "Hospital Name",
            covid_beds_this_week AS "COVID Cases This Week",
            covid_beds_last_week AS "COVID Cases Last Week",
            cases_difference AS "Difference in Cases"
        FROM ChangeInCases
        WHERE covid_beds_this_week IS NOT NULL AND
        covid_beds_last_week IS NOT NULL AND
        cases_difference IS NOT NULL AND
        covid_beds_last_week != 0
        ORDER BY cases_difference DESC
        LIMIT 10;
        """
        parameters = {'selected_week': selected_week,
                      'previous_week': selected_week - timedelta(weeks=1)}

        df_rpt_6 = pd.read_sql_query(q_rpt_6, conn, params=parameters)
        df_rpt_6.index = df_rpt_6.index + 1

        st.write("## 10 Hospitals with Biggest Weekly \
                 Difference in COVID Cases")
        st.dataframe(df_rpt_6.head(10), use_container_width=True)

    with tab7:
        # Report 7: Hospitals That Did Not Report Data
        q_rpt_7 = """
        WITH MostRecentReporting AS (
        SELECT
            hospital_name,
            hl.hospital_pk,
            MAX(collection_week) as most_recent_date
        FROM HospitalLogistics AS hl
        JOIN HospitalSpecificDetails AS hs
        ON hl.hospital_pk = hs.hospital_pk
        GROUP BY hospital_name, hl.hospital_pk
        ),
        NotReportedLastWeek AS (
        SELECT
            mr.hospital_name,
            mr.most_recent_date
        FROM MostRecentReporting mr
        WHERE NOT EXISTS (
            SELECT 1
            FROM HospitalLogistics hl
            WHERE hl.collection_week = %(previous_week)s
            AND hl.hospital_pk = mr.hospital_pk
        )
        )
        SELECT
            hospital_name AS "Hospital Name",
            most_recent_date AS "Last Reported Date"
        FROM NotReportedLastWeek
        WHERE hospital_name IS NOT NULL AND
        most_recent_date IS NOT NULL
        ORDER BY hospital_name
        """
        parameters = {'selected_week': selected_week,
                      'previous_week': selected_week - timedelta(weeks=1)}

        df_rpt_7 = pd.read_sql_query(q_rpt_7, conn, params=parameters)
        df_rpt_7.index = df_rpt_7.index + 1

        st.write("## Hospitals That Did Not Report Data For Selected Week")
        st.dataframe(df_rpt_7, use_container_width=True)

    # Close the database connection
    conn.close()


if __name__ == "__main__":
    main()
