-- queries.sql — Business questions & answers (short answers included)

-- Q1) Which doctor has the most confirmed appointments?
-- Short answer: Doctor with most confirmed appointments is doctor_id 100 ("Dr. Pérez") with 109 confirmed appointments.
SELECT d.doctor_id,
       d.name,
       COUNT(*) AS confirmed_appointments
FROM healthtech.appointments a
JOIN healthtech.doctors d ON d.doctor_id = a.doctor_id
WHERE a.status = 'confirmed'
GROUP BY d.doctor_id, d.name
ORDER BY confirmed_appointments DESC, d.doctor_id
LIMIT 1;

-- Q2) How many confirmed appointments does the patient with patient_id '34' have?
-- Short answer: 14
SELECT COUNT(*) AS confirmed_appointments_for_patient_34
FROM healthtech.appointments a
WHERE a.patient_id = 34 AND a.status = 'confirmed';

-- Q3) How many cancelled appointments are there between 2025-10-21 and 2025-10-24 (inclusive)?
-- Short answer: 32
SELECT COUNT(*) AS cancelled_in_window
FROM healthtech.appointments a
WHERE a.status = 'cancelled'
  AND a.booking_date BETWEEN DATE '2025-10-21' AND DATE '2025-10-24';

-- Q4) What is the total number of confirmed appointments for each doctor?
-- Short answer: see the full list returned by this query (top 5 preview from the cleaned dataset used during development):
-- [{'doctor_id': 100, 'confirmed_appointments': 109, 'name': 'Dr. Pérez', 'specialty': 'Vein'}, {'doctor_id': 102, 'confirmed_appointments': 86, 'name': 'Dr. Sánchez', 'specialty': 'Vein'}, {'doctor_id': 101, 'confirmed_appointments': 84, 'name': 'Dr. Gómez', 'specialty': 'Pain'}, {'doctor_id': 104, 'confirmed_appointments': 81, 'name': 'Dr. Martínez', 'specialty': 'Vein'}, {'doctor_id': 105, 'confirmed_appointments': 78, 'name': 'Unknown', 'specialty': 'Unknown'}]
SELECT d.doctor_id,
       d.name,
       COUNT(*) AS confirmed_appointments
FROM healthtech.appointments a
JOIN healthtech.doctors d ON d.doctor_id = a.doctor_id
WHERE a.status = 'confirmed'
GROUP BY d.doctor_id, d.name
ORDER BY confirmed_appointments DESC, d.doctor_id;
