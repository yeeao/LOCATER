package localizationDB;


import java.sql.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.time.temporal.ChronoUnit.*;

/*
* This class is created to store prediction result to a table.
*  preds_result table (id int, location varchar, probability float, result_list varchar)
*  - result list is location and probability pair. 'location':'prob','location':'prob',
*
*  TODO: use hash table to check if table was created or not. convert mac address to an unique number
* */
public class TableFiller {

    static int LEARN_TIME_LENGTH = 14;
    static String TARGET_TABLE = "prediction_result";
    static DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    static int April_2018_start_id = 19397381;
    static int ID_RANGE = 100;
    public static void main(String[] args) throws InterruptedException {
        Initialization.Initialize();
        // get range of ids from observation_clean
        long start = System.currentTimeMillis();

        for (int i = 0; i < 3; i++) {
            int start_id = getMaximumID();

            if (start_id == -1) {
                System.out.println("start ID error");
                return;
            }

            Callable<Void> call_1 = () -> {
                OneCall(start_id, start_id + ID_RANGE);
                return null;
            };

            Callable<Void> call_2 = () -> {
                OneCall(start_id + ID_RANGE + 1, start_id + ID_RANGE * 2);
                return null;
            };

            Callable<Void> call_3 = () -> {
                OneCall(start_id + ID_RANGE * 2 + 1, start_id + ID_RANGE * 3);
                return null;
            };

//        Callable<Void> call_4 = new Callable<Void>() {
//            @Override
//            public Void call() throws Exception {
//                OneCall(75, 100);
//                return null;
//            }
//        };

            List<Callable<Void>> tasks = new ArrayList<>();
            tasks.add(call_1);
            tasks.add(call_2);
            tasks.add(call_3);

            ExecutorService executorService = Executors.newFixedThreadPool(3);

            try {
                executorService.invokeAll(tasks);
            } catch (InterruptedException e) {

            }

            executorService.shutdownNow();
            if (!executorService.awaitTermination(100, TimeUnit.MICROSECONDS)) {
                System.out.println("Still waiting...");
                System.exit(0);
            }
            System.out.println("Exiting normally...");
        }
        long finish = System.currentTimeMillis();
        long timeElapsed = finish - start;
        System.out.println(timeElapsed);
    }

    private static void OneCall(int low, int high) {
        try(Connect connect = new Connect("local", "tippersdb_restored")) {
            Connection connection = connect.getConnection();
            PreparedStatement p = connection.prepareStatement(
                    String.format("SELECT * FROM observation_clean WHERE id >= %s and id <= %s;", low, high));
            ResultSet res = p.executeQuery();
            PreparedStatement p2 = connection.prepareStatement(String.format("insert into %s values(?,?,?,?,?,?)", TARGET_TABLE));

            String prev_hashed_mac = "";
            LocalDateTime prev_timestamp = LocalDateTime.now();
            while(res.next()) {
                // check if new row is not too close to previous row
                LocalDateTime time = LocalDateTime.parse(res.getString("timestamp"), formatter);
                if(prev_hashed_mac.equals(res.getString("payload")) && Math.abs(ChronoUnit.SECONDS.between(prev_timestamp, time)) < 10) {
                    // if mac is the same and time difference is within 30 seconds, skip this row
                    continue;
                } else {
                    prev_hashed_mac = res.getString("payload");
                    prev_timestamp = time;
                }

                // make prediction
                LocationSet locs = LocationPrediction.getLocation(res.getString("payload"),
                        res.getString("timestamp"), LEARN_TIME_LENGTH);

                System.out.println(locs.buildingLocation + " " + locs.regionLocation + " " + locs.roomLocation);

                Map<String, Double> loc_and_prob = new HashMap<>();
                double prob = 0;
                if(!locs.buildingLocation.equals("null")) {
                    for (int i = 0; i < locs.probabilities.size(); i++) {
                        loc_and_prob.put(locs.rooms.get(i), locs.probabilities.get(i));
                        if(Objects.equals(locs.roomLocation, locs.rooms.get(i))) {
                            prob = locs.probabilities.get(i);
                        }
                    }
                }

                String map_print = loc_and_prob.keySet().stream()
                        .map(key -> key + ":" + loc_and_prob.get(key))
                        .collect(Collectors.joining(", ", "{", "}"));
                System.out.println(map_print);

                p2.setInt(1, res.getInt("id"));
                p2.setString(2, locs.buildingLocation);
                p2.setString(3, locs.regionLocation);
                p2.setString(4, locs.roomLocation);
                p2.setDouble(5, prob);
                p2.setString(6, map_print);
                p2.addBatch();
            }

            // store result
            p2.executeBatch();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static int getMaximumID() {
        try(Connect connect = new Connect("local", "tippersdb_restored")) {
            Connection connection = connect.getConnection();
            PreparedStatement p = connection.prepareStatement(
                    String.format("SELECT MAX(observation_id) FROM %s;", TARGET_TABLE));
            ResultSet res = p.executeQuery();
            res.next();
            return res.getInt(1) + 1;
        } catch (Exception e) {

        }

        return -1;
    }
}
