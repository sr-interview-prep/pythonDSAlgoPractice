-- Database initialization script

-- Define STUDENTS table
CREATE TABLE STUDENTS (
    id INTEGER PRIMARY KEY,
    name VARCHAR(255),
    college_name VARCHAR(255)
);

-- Define PARTICIPATIONS table
CREATE TABLE PARTICIPATIONS (
    student_id INTEGER,
    category VARCHAR(255),
    score INTEGER,
    PRIMARY KEY (student_id, category),
    FOREIGN KEY (student_id) REFERENCES STUDENTS(id)
);

-- Insert sample data into STUDENTS
INSERT INTO STUDENTS (id, name, college_name) VALUES
(1, 'Alice Wonderland', 'State University'),
(2, 'Bob The Builder', 'City College'),
(3, 'Charlie Brown', 'State University'),
(4, 'Diana Prince', 'Tech Institute'),
(5, 'Eve Harrington', 'City College'),
(6, 'Frankenstein Monster', 'Tech Institute'),
(7, 'Grace Hopper', 'State University');

-- Insert sample data into PARTICIPATIONS
INSERT INTO PARTICIPATIONS (student_id, category, score) VALUES
-- Category: Coding
(1, 'Coding', 95),
(2, 'Coding', 90),
(3, 'Coding', 95),
(4, 'Coding', 88),
(5, 'Coding', 70),
(6, 'Coding', 88),
-- Category: Debate
(1, 'Debate', 80),
(2, 'Debate', 85),
(4, 'Debate', 90),
(5, 'Debate', 80),
(7, 'Debate', 75),
-- Category: Chess
(3, 'Chess', 100),
(5, 'Chess', 95),
(6, 'Chess', 98),
(7, 'Chess', 95),
-- Category: Robotics
(1, 'Robotics', 92),
(2, 'Robotics', 92),
(4, 'Robotics', 85),
(6, 'Robotics', 80),
(7, 'Robotics', 90),
-- Category: Math
(1, 'Math', 100),
(3, 'Math', 90),
(5, 'Math', 90);
