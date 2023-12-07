-- stg_pts_members.sql

WITH staging_data AS (
    SELECT
        MbrID AS Patient_ID,
        AddedBy_ID AS PatientAddedByUserID,
        TO_TIMESTAMP(Birthday) AS PatientBirthDate,
        MbrProvID AS PreferredProviderID,
        EnrolledBy_ID AS PatientEnrolledByUserID,
        FirstName AS PatientFirstName,
        Gender AS PatientGender,
        TO_TIMESTAMP(Timestamp) AS PatientLastEditDate,
        LastName AS PatientLastName,
        MailFlag AS PatientMailFlag,
        MbrAddress AS PatientAddress
    FROM {{ ref('members') }} -- Reference the source model 'members'
)
SELECT
    *
    -- Additional staging transformations if needed
FROM staging_data

