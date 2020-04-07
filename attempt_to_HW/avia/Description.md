## Airport

AIRPOPRT_ID = 0;
NAME = 1; //may be city
CITY = 2;
COUNTRY = 3;
IATA = 4; //nullable
ICAO = 5;//nullable
LAT = 6; //decimal
LON = 7; //decimal
Altitude = 8;
Timezone = 9;
DST = 10;
TZ = 11;
Type = 12; //only airport in airports.dat
Source = 13;

## Airline

AIRLINE_ID = 0;
NAME = 1;
Alias = 2;
IATA = 3;
ICAO = 4;
CallSign = 5;
Country = 6;
Active = 3; // Y/N

## Route

AIRLIE = 0; //IATA or ICAO of airline
AIRLIE_ID = 1;
SOURCE_AIRPORT = 2; //IATA or ICAO of airport
SOURCE_AIRPORT_ID = 3;
TARGET_AIRPORT = 4; //IATA or ICAO of airport
TARGET_AIRPORT_ID = 5;
CODESHARE = 6; //flight not operated by airline
STOPS = 7; //count of stops of flight
EQUIPMENT = 8; //plane type code

## Plane

NAME = 0;
IATA = 1;
ICAO = 2;

## Country

NAME = 0;
ISO_CODE = 1;
DAFIF_CODE = 2;