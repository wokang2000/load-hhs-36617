import queries
import psycopg
from psycopg import errors
import credentials


def main():
    conn = psycopg.connect(
        host="pinniped.postgres.database.azure.com",
        dbname=credentials.DB_USER,
        user=credentials.DB_USER,
        password=credentials.DB_PASSWORD
    )

    cur = conn.cursor()

    try:
        with conn.transaction():
            cur.execute(queries.HOSPITAL_SPECIFIC_DETAILS_CREATE_QUERY)
            print("Successfully created HospitalSpecificDetails table.")

            cur.execute(queries.HOSPITAL_LOGISTICS_CREATE_QUERY)
            print("Successfully created HospitalLogistics table.")

            cur.execute(queries.HOSPITAL_QUALITY_DETAILS_CREATE_QUERY)
            print("Successfully created HospitalQualityDetails table.")

    except errors.DatabaseError as e:
        print(f"Database error occurred: {e}")

    except Exception as e:
        print(f"An error occurred: {e}") 

    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    main()
