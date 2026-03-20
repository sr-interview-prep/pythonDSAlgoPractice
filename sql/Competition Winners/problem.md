# Competition Winners

## Description

A university holds various competitions across different categories.
The task is to identify the top three students in each category based on their scores.
If there's a tie in scores, students should be ranked alphabetically by their college name.

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

## Task

Write a SQL query to output the `category`, `student_id`, `name`, `college_name`, and `score`
for the students who ranked in the top 3 of each `category`.
The results should be ordered by `category`, then `student_id`, `name`, `college_name`, and `score`.
