package localizationDB;

import java.util.*;

public class DBHLocationMap {

    public static Map<Integer, Integer> locationMap = new HashMap<>();
    public static int numLocations;
    static {

        List<Integer> locations = new ArrayList<Integer>(
                Arrays.asList(
                    1100,1200,1300,1403,1406,1407,1412,1413,1420,1422,1423,1425,1427,1428,1429,1431,1433,1434,1500,1600,2002,2004,2008,2011,2013,2019,2026,2028,
                    2029,2032,2038,2039,2042,2044,2048,2051,2052,2054,2056,2058,2059,2061,2062,2064,2065,2066,2068,2069,2072,2074,2076,2081,2082,2084,2086,2088,
                    2089,2091,2092,2099,2202,2204,2206,2208,2209,2211,2212,2214,2216,2219,2221,2222,2224,2226,2228,2231,2232,2234,2241,2243,3002,3004,3008,3011,
                    3013,3019,3026,3028,3029,3032,3038,3039,3042,3044,3048,3051,3052,3054,3056,3058,3059,3061,3062,3064,3065,3066,3068,3069,3072,3074,3076,
                    3081,3082,3084,3086,3088,3089,3091,3092,3099,3202,3204,3206,3208,3209,3211,3212,3214,3216,3219,3221,3222,3224,3226,3228,3231,3232,3234,3241,
                    3243,4002,4004,4008,4011,4013,4019,4026,4028,4029,4032,4038,4039,4042,4044,4048,4051,4052,4054,4056,4058,4059,4061,4062,4064,4065,4066,4068,
                    4069,4072,4074,4076,4081,4082,4084,4086,4088,4089,4091,4092,4099,4202,4204,4206,4208,4209,4211,4212,4214,4216,4219,4221,4222,4224,4226,4228,
                    4231,4232,4234,4241,4243,5002,5004,5008,5011,5013,5019,5026,5028,5029,5032,5038,5039,5042,5044,5048,5051,5052,5054,5056,5058,5059,5061,5062,
                    5064,5065,5066,5068,5069,5072,5074,5076,5081,5082,5084,5086,5088,5089,5091,5092,5099,5202,5204,5206,5208,5209,5211,5212,5214,5216,5219,
                    5221,5222,5224,5226,5228,5231,5232,5234,5241,5243,6011,6013,6019,6024,6026,6028,6029,6032,6034,6036,6039,6042,6044,6046,6048,6049,6051,6052,
                    6056,6062,6064,6066,6068,6072,6074,6076,6082,6084,6086,6088,6091,6092,6122,6131,6132,6136,6210,6211,6212,6213,6215,6217,6218,6219
                )
        );
        numLocations = locations.size();
        locationMap = new HashMap<>();

        for (int i=0; i<numLocations; i++) {
            locationMap.put(locations.get(i), i);
        }

    }


}
