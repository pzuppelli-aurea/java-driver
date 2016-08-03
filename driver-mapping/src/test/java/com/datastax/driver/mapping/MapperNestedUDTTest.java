/*
 *      Copyright (C) 2012-2015 DataStax Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package com.datastax.driver.mapping;

import com.datastax.driver.core.CCMTestsSupport;
import com.datastax.driver.core.utils.CassandraVersion;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;
import com.datastax.driver.mapping.annotations.UDT;
import org.testng.annotations.Test;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@SuppressWarnings({"unused"})
@CassandraVersion(major = 2.1)
public class MapperNestedUDTTest extends CCMTestsSupport {

    @Override
    public void onTestContextInitialized() {
        execute(
                "CREATE TYPE point (x int, y int)",
                "CREATE TYPE rectangle (a frozen<point>, b frozen<point>, c frozen<point>, d frozen<point>)",
                "CREATE TABLE user (id uuid PRIMARY KEY, name text, area frozen<rectangle>)"
        );
    }

    /**
     * Validates that tables having a UDT column that itself has a UDT field can be handled by the object mapper.
     *
     * @jira_ticket JAVA-1255
     * @test_category object_mapper
     */
    @Test(groups = "short")
    public void should_be_able_to_create_entity_from_table_having_udt_with_nested_udt() {
        MappingManager manager = new MappingManager(session());
        Mapper<User> mapper = manager.mapper(User.class);

        Rectangle r = new Rectangle(
                new Point(10, 0),
                new Point(10, 10),
                new Point(0, 10),
                new Point(0, 0)
        );

        User user = new User();
        user.setId(UUID.randomUUID());
        user.setName("Bob");
        user.setArea(r);

        mapper.save(user);

        User retrieved = mapper.get(user.getId());
        assertThat(retrieved).isEqualTo(user);
    }

    @Table(name = "user")
    public static class User {
        @PartitionKey
        private UUID id;

        private String name;

        private Rectangle area;

        public UUID getId() {
            return id;
        }

        public void setId(UUID id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public Rectangle getArea() {
            return area;
        }

        public void setArea(Rectangle area) {
            this.area = area;
        }

        @Override
        public String toString() {
            return "User{" +
                    "id=" + id +
                    ", name='" + name + '\'' +
                    ", area=" + area +
                    '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof User)) return false;

            User user = (User) o;

            if (id != null ? !id.equals(user.id) : user.id != null) return false;
            if (name != null ? !name.equals(user.name) : user.name != null) return false;
            return area != null ? area.equals(user.area) : user.area == null;

        }

        @Override
        public int hashCode() {
            int result = id != null ? id.hashCode() : 0;
            result = 31 * result + (name != null ? name.hashCode() : 0);
            result = 31 * result + (area != null ? area.hashCode() : 0);
            return result;
        }
    }


    @UDT(name = "rectangle")
    public static class Rectangle {
        private Point a;
        private Point b;
        private Point c;
        private Point d;

        public Rectangle(Point a, Point b, Point c, Point d) {
            this.a = a;
            this.b = b;
            this.c = c;
            this.d = d;
        }

        public Rectangle() {

        }

        public Point getA() {
            return a;
        }

        public void setA(Point a) {
            this.a = a;
        }

        public Point getB() {
            return b;
        }

        public void setB(Point b) {
            this.b = b;
        }

        public Point getC() {
            return c;
        }

        public void setC(Point c) {
            this.c = c;
        }

        public Point getD() {
            return d;
        }

        public void setD(Point d) {
            this.d = d;
        }

        @Override
        public String toString() {
            return "Rectangle{" +
                    "a=" + a +
                    ", b=" + b +
                    ", c=" + c +
                    ", d=" + d +
                    '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof Rectangle)) return false;

            Rectangle rectangle = (Rectangle) o;

            if (a != null ? !a.equals(rectangle.a) : rectangle.a != null) return false;
            if (b != null ? !b.equals(rectangle.b) : rectangle.b != null) return false;
            if (c != null ? !c.equals(rectangle.c) : rectangle.c != null) return false;
            return d != null ? d.equals(rectangle.d) : rectangle.d == null;

        }

        @Override
        public int hashCode() {
            int result = a != null ? a.hashCode() : 0;
            result = 31 * result + (b != null ? b.hashCode() : 0);
            result = 31 * result + (c != null ? c.hashCode() : 0);
            result = 31 * result + (d != null ? d.hashCode() : 0);
            return result;
        }
    }


    @UDT(name = "point")
    public static class Point {
        private int x;
        private int y;

        public Point(int x, int y) {
            this.x = x;
            this.y = y;
        }

        public Point() {

        }

        public int getX() {
            return x;
        }

        public void setX(int x) {
            this.x = x;
        }

        public int getY() {
            return y;
        }

        public void setY(int y) {
            this.y = y;
        }

        @Override
        public String toString() {
            return "Point{" +
                    "x=" + x +
                    ", y=" + y +
                    '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof Point)) return false;

            Point point = (Point) o;

            if (x != point.x) return false;
            return y == point.y;

        }

        @Override
        public int hashCode() {
            int result = x;
            result = 31 * result + y;
            return result;
        }
    }


}
