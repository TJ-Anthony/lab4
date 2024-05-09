package edu.canisius.cyb600.lab4.database;

import edu.canisius.cyb600.lab4.dataobjects.Actor;
import edu.canisius.cyb600.lab4.dataobjects.Category;
import edu.canisius.cyb600.lab4.dataobjects.Film;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Posgres Implementation of the db adapter.
 */
public class PostgresDBAdapter extends AbstractDBAdapter {

    public PostgresDBAdapter(Connection conn) {
        super(conn);
    }

    @Override
    public List<String> getAllDistinctCategoryNames() {
        //Create a string with the sql statement
        String sql = "Select DISTINCT NAME FROM CATEGORY";
        List<String> categories = new ArrayList<>();
        //Prepare the SQL statement with the code
        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            ResultSet results = statement.executeQuery();
            //Loop through all the results and create a new Film object to hold all its information
            while (results.next()) {
                String category = results.getString("NAME");
                categories.add(category);
            }
        } catch (SQLException exception) {
            exception.printStackTrace();
        }
        return categories;
    }

    @Override
    public List<Film> getAllFilmsWithALengthLongerThanX(int length) {
        String sql = "Select * FROM FILM WHERE length > ?";
        List<Film> films = new ArrayList<>();
        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            //Substitute a string for last name for the ? in the sql
            statement.setInt(1, length);

            ResultSet results = statement.executeQuery();
            //Initialize an empty List to hold the return set of films.

            //Loop through all the results and create a new Film object to hold all its information
            while (results.next()) {
                Film film = new Film();
                film.setFilmId(results.getInt("FILM_ID"));
                film.setTitle(results.getString("TITLE"));
                film.setDescription(results.getString("DESCRIPTION"));
                film.setReleaseYear(results.getString("RELEASE_YEAR"));
                film.setLanguageId(results.getInt("LANGUAGE_ID"));
                film.setOriginalLanguageId(results.getInt("ORIGINAL_LANGUAGE_ID"));
                film.setRentalDuration(results.getInt("RENTAL_DURATION"));
                film.setRentalRate(results.getDouble("RENTAL_RATE"));
                film.setLength(results.getInt("LENGTH"));
                film.setReplacementCost(results.getDouble("REPLACEMENT_COST"));
                film.setRating(results.getString("RATING"));
                film.setSpecialFeatures(results.getString("SPECIAL_FEATURES"));
                film.setLastUpdate(results.getDate("LAST_UPDATE"));

                //Add film to the array
                films.add(film);
            }

        } catch (SQLException exception) {
            exception.printStackTrace();
        }
        return films;
    }


    @Override
    public List<Actor> getActorsFirstNameStartingWithX(char firstLetter) {
        //Create a string with the sql statement
        String sql = "Select * from actor where First_Name LIKE ?";
        List<Actor> actors = new ArrayList<>();
        //Prepare the SQL statement with the code
        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            //Substitute a string for last name for the ? in the sql
            statement.setString(1, firstLetter + "%");
            ResultSet results = statement.executeQuery();
            //Loop through all the results and create a new Film object to hold all its information
            while (results.next()) {
                Actor actor = new Actor();
                actor.setActorId(results.getInt("ACTOR_ID"));
                actor.setFirstName(results.getString("FIRST_NAME"));
                actor.setLastName(results.getString("LAST_NAME"));
                actor.setLastUpdate(results.getDate("LAST_UPDATE"));

                //Add film to the array
                actors.add(actor);
            }
        } catch (SQLException exception) {
            exception.printStackTrace();
        }
        return actors;
    }

    @Override
    public List<Film> getFilmsInCategory(Category category) {
        String sql = "select *\n" +
                "from category, film_category, film\n" +
                "where category.category_id = film_category.category_id\n" +
                "and film.film_id = film_category.film_id\n" +
                "and category.category_id = ?\n";
        List<Film> films = new ArrayList<>();

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            //Substitute a string for last name for the ? in the sql
            statement.setInt(1, category.getCategoryId());

            ResultSet results = statement.executeQuery();
            //Initialize an empty List to hold the return set of films.

            //Loop through all the results and create a new Film object to hold all its information
            while (results.next()) {
                Film film = new Film();
                film.setFilmId(results.getInt("FILM_ID"));
                film.setTitle(results.getString("TITLE"));
                film.setDescription(results.getString("DESCRIPTION"));
                film.setReleaseYear(results.getString("RELEASE_YEAR"));
                film.setLanguageId(results.getInt("LANGUAGE_ID"));
                film.setRentalDuration(results.getInt("RENTAL_DURATION"));
                film.setRentalRate(results.getDouble("RENTAL_RATE"));
                film.setLength(results.getInt("LENGTH"));
                film.setReplacementCost(results.getDouble("REPLACEMENT_COST"));
                film.setRating(results.getString("RATING"));
                film.setSpecialFeatures(results.getString("SPECIAL_FEATURES"));
                film.setLastUpdate(results.getDate("LAST_UPDATE"));
                //Add film to the array
                films.add(film);
            }

        } catch (SQLException exception) {
            exception.printStackTrace();
        }
        return films;
    }


    @Override
    public List<Actor> insertAllActorsWithAnOddNumberLastName(List<Actor> actors) {
        String sql = "INSERT INTO ACTOR (first_name, last_name) VALUES (? , ? ) returning ACTOR_ID, LAST_UPDATE";
        List<Actor> oddActors = new ArrayList<>();
        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            for (Actor actor : actors) {
                if (actor.getLastName().length() % 2 != 0) {
                    statement.setString(1, actor.getFirstName().toUpperCase());
                    statement.setString(2, actor.getLastName().toUpperCase());
                    ResultSet results = statement.executeQuery();
                    if (results.next()) {
                        actor.setActorId(results.getInt("ACTOR_ID"));
                        actor.setLastUpdate(results.getDate("LAST_UPDATE"));
                        oddActors.add(actor);
                    }
                }
            }
                } catch (SQLException exception) {
                    exception.printStackTrace();
                }
                return oddActors;
            }


}



