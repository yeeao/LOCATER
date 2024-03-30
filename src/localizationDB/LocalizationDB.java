// This code is used for testing 
/*
 * 1. for the format of email, deal with it as a black box: differential them inside functions
 */

package localizationDB;

import service.APtoRoom;
import service.LocationService;
import service.LocationState;
import service.RoomToAP;

import java.sql.Connection;

import dao.LocalDataGeneration;

import java.time.format.DateTimeFormatter;

public class LocalizationDB {
	public static void main(String args[]) {
		Initialization.Initialize();

		// added by Seiya
		DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

		//long startTime = System.currentTimeMillis();
		System.out.println(java.time.LocalDateTime.now().format(dtf));
		LocationSet Location = LocationPrediction.getLocation("d5d1972a21856639df45695de00a69eb6644eb26","2018-05-01 12:34:11",14);
//		LocationSet Location = LocationPrediction.getLocation("d5d1972a21856639df45695de00a69eb6644eb26",java.time.LocalDateTime.now().format(dtf),14);
//		LocationSet Location = LocationPrediction.getLocation("b8076f28416d9666c60b6989da74addcd521b96a","2019-04-30 13:59:00",14);
		//long endTime = System.currentTimeMillis();
		//long duration = (endTime - startTime);

		//System.out.println(duration);
		System.out.println(Location.buildingLocation + " " + Location.regionLocation + " " + Location.roomLocation);

		double probs[] = new double[DBHLocationMap.numLocations];
		int label = 0;
		double max_ = 0.0;
		if (!Location.buildingLocation.equals("null")) {
			System.out.println(Location.probabilities.size() + " " + Location.rooms.size());
			for (int i = 0; i < Location.probabilities.size(); i++) {
				try {
					probs[DBHLocationMap.locationMap.get(
							Integer.parseInt(Location.rooms.get(i)))] = Location.probabilities.get(i);
					System.out.println(Location.rooms.get(i) + " " + Location.probabilities.get(i));
					if ( Location.probabilities.get(i) > max_) {
						label = DBHLocationMap.locationMap.get(
								Integer.parseInt(Location.rooms.get(i)));
						max_ = Location.probabilities.get(i);
					}
				} catch (Exception ignored) {

				}
			}
		}
	}

}
