# Problem: Students Table Initialization

## Description

This exercise provides the SQL schema and sample data for the `STUDENTS` and `PARTICIPATIONS` tables, which are used in other exercises. You can use this to initialize your database for SQL practice.

## Task

Run the provided SQL to create the tables and insert the sample data.

---

## Tables

### STUDENTS

- `id` (INTEGER, PRIMARY KEY): Unique identifier for the student.
- `name` (VARCHAR): Name of the student.
- `college_name` (VARCHAR): Name of the student's college.

### PARTICIPATIONS

- `student_id` (INTEGER): Foreign key referencing STUDENTS(id).
- `category` (VARCHAR): The category of the competition.
- `score` (INTEGER): The score obtained by the student in that category.
- PRIMARY KEY (`student_id`, `category`)

---

## Sample Data

See the solution.sql file for the schema and data.
