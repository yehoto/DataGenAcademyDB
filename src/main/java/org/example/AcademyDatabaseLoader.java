package org.example;

import java.sql.*;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.*;
import java.util.stream.IntStream;
import java.util.stream.Collectors;

public class AcademyDatabaseLoader {
    private static final String URL = "jdbc:postgresql://localhost:5433/academy";
    private static final String USER = "anna";
    private static final String PASSWORD = "qwerty";

    static class Student {
        int id;
        String name;
        int startYear;
        List<Exam> exams = new ArrayList<>();

        public Student(int id, String name, int startYear) {
            this.id = id;
            this.name = name;
            this.startYear = startYear;
        }
    }

    static class Course {
        int id;
        String title;
        int hours;

        public Course(int id, String title, int hours) {
            this.id = id;
            this.title = title;
            this.hours = hours;
        }
    }

    static class Exam {
        int studentId;
        int courseId;
        BigDecimal score;

        public Exam(int studentId, int courseId, BigDecimal score) {
            this.studentId = studentId;
            this.courseId = courseId;
            this.score = score;
        }
    }

    public static void main(String[] args) {
        try (Connection conn = DriverManager.getConnection(URL, USER, PASSWORD)) {
            conn.setAutoCommit(false);// Включаем ручное управление транзакциями

            clearTables(conn);

            List<Student> students = generateStudents(100);
            List<Course> courses = generateCourses(10);
            List<Exam> allExams = generateExams(students, courses);

            saveCourses(conn, courses);
            saveStudents(conn, students);
            saveExams(conn, allExams);

            conn.commit();// Теперь все изменения фиксируются разом
            System.out.println("Данные успешно загружены в БД");
        } catch (SQLException e) {
            System.err.println("Ошибка работы с БД: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private static void clearTables(Connection conn) throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.executeUpdate("TRUNCATE TABLE Exams, Students, Courses RESTART IDENTITY;");
        }
    }

    private static List<Student> generateStudents(int count) {
        Random random = new Random();
        return IntStream.rangeClosed(1, count)//Создание потока чисел от 1 до count
                .mapToObj(i -> new Student(//Преобразование каждого числа в объект Student
                        i,// id студента = текущее число (i)
                        "Student_" + (1 + random.nextInt(10)),// случайное имя
                        2020 + random.nextInt(5)// случайный год в периоде 2020-2024
                ))
                .collect(Collectors.toCollection(ArrayList::new));//Собирает все объекты Course в изменяемый список ArrayList
    }

    private static List<Course> generateCourses(int count) {
        Random random = new Random();
        return IntStream.rangeClosed(1, count)
                .mapToObj(i -> new Course(
                        i,
                        "Course_" + i,
                        10 + random.nextInt(51)// диапазон часов 10-60
                ))
                .collect(Collectors.toCollection(ArrayList::new));
    }

    private static List<Exam> generateExams(List<Student> students, List<Course> courses) {
        Random random = new Random();
        List<Exam> allExams = new ArrayList<>();
        // Для каждого студента
        students.forEach(student -> {
            int examsCount = random.nextInt(7);// Количество экзаменов: 0-6 (включительно)
            for (int i = 0; i < examsCount; i++) {// Генерируем examsCount экзаменов
                Course randomCourse = courses.get(random.nextInt(courses.size())); // Выбираем случайный курс (возможны повторения)
                BigDecimal score = BigDecimal.valueOf(random.nextDouble() * 100)// Генерируем оценку (0.00 - 100.00)
                        .setScale(2, RoundingMode.HALF_UP);// округляем до 2-х знаков после запятой
                allExams.add(new Exam(student.id, randomCourse.id, score));
            }
        });

        Collections.shuffle(allExams);// перемешиваем, чтобы экзамены не были сгруппированы по студентам
        return allExams;  // Возвращаем перемешанный список
    }

    private static void saveCourses(Connection conn, List<Course> courses) throws SQLException {
        String sql = "INSERT INTO Courses (c_no, title, hours) VALUES (?, ?, ?)";
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            for (Course course : courses) {
                pstmt.setInt(1, course.id);
                pstmt.setString(2, course.title);
                pstmt.setInt(3, course.hours);
                pstmt.addBatch();//добавляет запрос в "пакет" (batch) - это значит, что запрос не выполняется сразу, а добавляется в группу для выполнения позже
            }
            pstmt.executeBatch();// Все запросы отправляются, но не коммитятся
        }
    }

    private static void saveStudents(Connection conn, List<Student> students) throws SQLException {
        String sql = "INSERT INTO Students (s_id, name, start_year) VALUES (?, ?, ?)";
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            for (Student student : students) {
                pstmt.setInt(1, student.id);
                pstmt.setString(2, student.name);
                pstmt.setInt(3, student.startYear);
                pstmt.addBatch();
            }
            pstmt.executeBatch();
        }
    }

    private static void saveExams(Connection conn, List<Exam> allExams) throws SQLException {
        String sql = "INSERT INTO Exams (s_id, c_no, score) VALUES (?, ?, ?)";
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            for (Exam exam : allExams) {  // Итерируем напрямую по перемешанным экзаменам
                pstmt.setInt(1, exam.studentId);
                pstmt.setInt(2, exam.courseId);
                pstmt.setBigDecimal(3, exam.score);
                pstmt.addBatch();
            }
            pstmt.executeBatch();
        }
    }
}