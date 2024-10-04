package main

import (
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/bytedance/gopkg/lang/fastrand"
)

type Decimal1 = int

type weatherStationSource struct {
	name string
	avg  Decimal1
}

const MEASUREMENT_DIVERGENCE = 109

func main() {
	for _, station := range SOURCE_STATIONS {
		s := 0
		n := station.name[:len(station.name)-1]
		for _, b := range []byte(n) {
			s += int(b)
		}
	}
	if len(os.Args) < 2 {
		panic("missing parameter: number of records to create (int)")
	}
	count, err := strconv.ParseInt(os.Args[1], 10, 64)
	if err != nil {
		panic("invalid parameter: number of records to create (int)")
	}
	maxRecordLength := 0
	maxLengthAfterName := len("-99.9\n")
	for _, ws := range SOURCE_STATIONS {
		maxRecordLength = max(maxRecordLength, len(ws.name)+maxLengthAfterName)
	}
	maxFileSize := count * int64(maxRecordLength)
	f, err := os.Create("measurements.txt")
	if err != nil {
		panic(err)
	}
	defer f.Close()
	f.Truncate(maxFileSize)
	written := int64(0)
	nextTick := time.Now().Add(time.Second)
	fmt.Println("Generating measurements.txt...")
	for i := range count {
		now := time.Now()
		if now.After(nextTick) {
			fmt.Printf("\r%0.2f%%", (float64(i) / (float64(count) / 100)))
			nextTick = now.Add(time.Second)
		}
		station := SOURCE_STATIONS[fastrand.Int()%len(SOURCE_STATIONS)]
		measurement := station.avg + fastrand.Int()%(MEASUREMENT_DIVERGENCE*2+1) - MEASUREMENT_DIVERGENCE
		measurement = max(-999, min(999, measurement))
		if n, err := writeMeasurement(f, station.name, measurement); err != nil {
			panic(err)
		} else {
			written += int64(n)
		}
	}
	fmt.Println("100.00%")
	f.Truncate(written)
}

func writeMeasurement(f *os.File, stationName string, measurement int) (int, error) {
	var written int
	if n, err := f.Write([]byte(stationName)); err != nil {
		return 0, err
	} else {
		written = n
	}
	measurementBuffer := [6]byte{}
	bufPos := 0
	if measurement < 0 {
		measurementBuffer[0] = '-'
		bufPos = 1
		measurement = -measurement
	}
	scale := 100
	if measurement < 100 {
		scale = 10
	}
	for scale > 1 {
		measurementBuffer[bufPos] = '0' + byte(measurement/scale)
		measurement %= scale
		scale /= 10
		bufPos++
	}
	measurementBuffer[bufPos] = '.'
	measurementBuffer[bufPos+1] = '0' + byte(measurement)
	measurementBuffer[bufPos+2] = '\n'
	bufPos += 3
	if n, err := f.Write(measurementBuffer[:bufPos]); err != nil {
		return 0, err
	} else {
		return written + n, nil
	}
}

var SOURCE_STATIONS = [...]weatherStationSource{
	{"Abha;", 180},
	{"Abidjan;", 260},
	{"Abéché;", 294},
	{"Accra;", 264},
	{"Addis Ababa;", 160},
	{"Adelaide;", 173},
	{"Aden;", 291},
	{"Ahvaz;", 254},
	{"Albuquerque;", 140},
	{"Alexandra;", 110},
	{"Alexandria;", 200},
	{"Algiers;", 182},
	{"Alice Springs;", 210},
	{"Almaty;", 100},
	{"Amsterdam;", 102},
	{"Anadyr;", -69},
	{"Anchorage;", 28},
	{"Andorra la Vella;", 98},
	{"Ankara;", 120},
	{"Antananarivo;", 179},
	{"Antsiranana;", 252},
	{"Arkhangelsk;", 13},
	{"Ashgabat;", 171},
	{"Asmara;", 156},
	{"Assab;", 305},
	{"Astana;", 35},
	{"Athens;", 192},
	{"Atlanta;", 170},
	{"Auckland;", 152},
	{"Austin;", 207},
	{"Baghdad;", 228},
	{"Baguio;", 195},
	{"Baku;", 151},
	{"Baltimore;", 131},
	{"Bamako;", 278},
	{"Bangkok;", 286},
	{"Bangui;", 260},
	{"Banjul;", 260},
	{"Barcelona;", 182},
	{"Bata;", 251},
	{"Batumi;", 140},
	{"Beijing;", 129},
	{"Beirut;", 209},
	{"Belgrade;", 125},
	{"Belize City;", 267},
	{"Benghazi;", 199},
	{"Bergen;", 77},
	{"Berlin;", 103},
	{"Bilbao;", 147},
	{"Birao;", 265},
	{"Bishkek;", 113},
	{"Bissau;", 270},
	{"Blantyre;", 222},
	{"Bloemfontein;", 156},
	{"Boise;", 114},
	{"Bordeaux;", 142},
	{"Bosaso;", 300},
	{"Boston;", 109},
	{"Bouaké;", 260},
	{"Bratislava;", 105},
	{"Brazzaville;", 250},
	{"Bridgetown;", 270},
	{"Brisbane;", 214},
	{"Brussels;", 105},
	{"Bucharest;", 108},
	{"Budapest;", 113},
	{"Bujumbura;", 238},
	{"Bulawayo;", 189},
	{"Burnie;", 131},
	{"Busan;", 150},
	{"Cabo San Lucas;", 239},
	{"Cairns;", 250},
	{"Cairo;", 214},
	{"Calgary;", 44},
	{"Canberra;", 131},
	{"Cape Town;", 162},
	{"Changsha;", 174},
	{"Charlotte;", 161},
	{"Chiang Mai;", 258},
	{"Chicago;", 98},
	{"Chihuahua;", 186},
	{"Chișinău;", 102},
	{"Chittagong;", 259},
	{"Chongqing;", 186},
	{"Christchurch;", 122},
	{"City of San Marino;", 118},
	{"Colombo;", 274},
	{"Columbus;", 117},
	{"Conakry;", 264},
	{"Copenhagen;", 91},
	{"Cotonou;", 272},
	{"Cracow;", 93},
	{"Da Lat;", 179},
	{"Da Nang;", 258},
	{"Dakar;", 240},
	{"Dallas;", 190},
	{"Damascus;", 170},
	{"Dampier;", 264},
	{"Dar es Salaam;", 258},
	{"Darwin;", 276},
	{"Denpasar;", 237},
	{"Denver;", 104},
	{"Detroit;", 100},
	{"Dhaka;", 259},
	{"Dikson;", -111},
	{"Dili;", 266},
	{"Djibouti;", 299},
	{"Dodoma;", 227},
	{"Dolisie;", 240},
	{"Douala;", 267},
	{"Dubai;", 269},
	{"Dublin;", 98},
	{"Dunedin;", 111},
	{"Durban;", 206},
	{"Dushanbe;", 147},
	{"Edinburgh;", 93},
	{"Edmonton;", 42},
	{"El Paso;", 181},
	{"Entebbe;", 210},
	{"Erbil;", 195},
	{"Erzurum;", 51},
	{"Fairbanks;", -23},
	{"Fianarantsoa;", 179},
	{"Flores,  Petén;", 264},
	{"Frankfurt;", 106},
	{"Fresno;", 179},
	{"Fukuoka;", 170},
	{"Gabès;", 195},
	{"Gaborone;", 210},
	{"Gagnoa;", 260},
	{"Gangtok;", 152},
	{"Garissa;", 293},
	{"Garoua;", 283},
	{"George Town;", 279},
	{"Ghanzi;", 214},
	{"Gjoa Haven;", -144},
	{"Guadalajara;", 209},
	{"Guangzhou;", 224},
	{"Guatemala City;", 204},
	{"Halifax;", 75},
	{"Hamburg;", 97},
	{"Hamilton;", 138},
	{"Hanga Roa;", 205},
	{"Hanoi;", 236},
	{"Harare;", 184},
	{"Harbin;", 50},
	{"Hargeisa;", 217},
	{"Hat Yai;", 270},
	{"Havana;", 252},
	{"Helsinki;", 59},
	{"Heraklion;", 189},
	{"Hiroshima;", 163},
	{"Ho Chi Minh City;", 274},
	{"Hobart;", 127},
	{"Hong Kong;", 233},
	{"Honiara;", 265},
	{"Honolulu;", 254},
	{"Houston;", 208},
	{"Ifrane;", 114},
	{"Indianapolis;", 118},
	{"Iqaluit;", -93},
	{"Irkutsk;", 10},
	{"Istanbul;", 139},
	{"İzmir;", 179},
	{"Jacksonville;", 203},
	{"Jakarta;", 267},
	{"Jayapura;", 270},
	{"Jerusalem;", 183},
	{"Johannesburg;", 155},
	{"Jos;", 228},
	{"Juba;", 278},
	{"Kabul;", 121},
	{"Kampala;", 200},
	{"Kandi;", 277},
	{"Kankan;", 265},
	{"Kano;", 264},
	{"Kansas City;", 125},
	{"Karachi;", 260},
	{"Karonga;", 244},
	{"Kathmandu;", 183},
	{"Khartoum;", 299},
	{"Kingston;", 274},
	{"Kinshasa;", 253},
	{"Kolkata;", 267},
	{"Kuala Lumpur;", 273},
	{"Kumasi;", 260},
	{"Kunming;", 157},
	{"Kuopio;", 34},
	{"Kuwait City;", 257},
	{"Kyiv;", 84},
	{"Kyoto;", 158},
	{"La Ceiba;", 262},
	{"La Paz;", 237},
	{"Lagos;", 268},
	{"Lahore;", 243},
	{"Lake Havasu City;", 237},
	{"Lake Tekapo;", 87},
	{"Las Palmas de Gran Canaria;", 212},
	{"Las Vegas;", 203},
	{"Launceston;", 131},
	{"Lhasa;", 76},
	{"Libreville;", 259},
	{"Lisbon;", 175},
	{"Livingstone;", 218},
	{"Ljubljana;", 109},
	{"Lodwar;", 293},
	{"Lomé;", 269},
	{"London;", 113},
	{"Los Angeles;", 186},
	{"Louisville;", 139},
	{"Luanda;", 258},
	{"Lubumbashi;", 208},
	{"Lusaka;", 199},
	{"Luxembourg City;", 93},
	{"Lviv;", 78},
	{"Lyon;", 125},
	{"Madrid;", 150},
	{"Mahajanga;", 263},
	{"Makassar;", 267},
	{"Makurdi;", 260},
	{"Malabo;", 263},
	{"Malé;", 280},
	{"Managua;", 273},
	{"Manama;", 265},
	{"Mandalay;", 280},
	{"Mango;", 281},
	{"Manila;", 284},
	{"Maputo;", 228},
	{"Marrakesh;", 196},
	{"Marseille;", 158},
	{"Maun;", 224},
	{"Medan;", 265},
	{"Mek'ele;", 227},
	{"Melbourne;", 151},
	{"Memphis;", 172},
	{"Mexicali;", 231},
	{"Mexico City;", 175},
	{"Miami;", 249},
	{"Milan;", 130},
	{"Milwaukee;", 89},
	{"Minneapolis;", 78},
	{"Minsk;", 67},
	{"Mogadishu;", 271},
	{"Mombasa;", 263},
	{"Monaco;", 164},
	{"Moncton;", 61},
	{"Monterrey;", 223},
	{"Montreal;", 68},
	{"Moscow;", 58},
	{"Mumbai;", 271},
	{"Murmansk;", 06},
	{"Muscat;", 280},
	{"Mzuzu;", 177},
	{"N'Djamena;", 283},
	{"Naha;", 231},
	{"Nairobi;", 178},
	{"Nakhon Ratchasima;", 273},
	{"Napier;", 146},
	{"Napoli;", 159},
	{"Nashville;", 154},
	{"Nassau;", 246},
	{"Ndola;", 203},
	{"New Delhi;", 250},
	{"New Orleans;", 207},
	{"New York City;", 129},
	{"Ngaoundéré;", 220},
	{"Niamey;", 293},
	{"Nicosia;", 197},
	{"Niigata;", 139},
	{"Nouadhibou;", 213},
	{"Nouakchott;", 257},
	{"Novosibirsk;", 17},
	{"Nuuk;", -14},
	{"Odesa;", 107},
	{"Odienné;", 260},
	{"Oklahoma City;", 159},
	{"Omaha;", 106},
	{"Oranjestad;", 281},
	{"Oslo;", 57},
	{"Ottawa;", 66},
	{"Ouagadougou;", 283},
	{"Ouahigouya;", 286},
	{"Ouarzazate;", 189},
	{"Oulu;", 27},
	{"Palembang;", 273},
	{"Palermo;", 185},
	{"Palm Springs;", 245},
	{"Palmerston North;", 132},
	{"Panama City;", 280},
	{"Parakou;", 268},
	{"Paris;", 123},
	{"Perth;", 187},
	{"Petropavlovsk-Kamchatsky;", 19},
	{"Philadelphia;", 132},
	{"Phnom Penh;", 283},
	{"Phoenix;", 239},
	{"Pittsburgh;", 108},
	{"Podgorica;", 153},
	{"Pointe-Noire;", 261},
	{"Pontianak;", 277},
	{"Port Moresby;", 269},
	{"Port Sudan;", 284},
	{"Port Vila;", 243},
	{"Port-Gentil;", 260},
	{"Portland (OR);", 124},
	{"Porto;", 157},
	{"Prague;", 84},
	{"Praia;", 244},
	{"Pretoria;", 182},
	{"Pyongyang;", 108},
	{"Rabat;", 172},
	{"Rangpur;", 244},
	{"Reggane;", 283},
	{"Reykjavík;", 43},
	{"Riga;", 62},
	{"Riyadh;", 260},
	{"Rome;", 152},
	{"Roseau;", 262},
	{"Rostov-on-Don;", 99},
	{"Sacramento;", 163},
	{"Saint Petersburg;", 58},
	{"Saint-Pierre;", 57},
	{"Salt Lake City;", 116},
	{"San Antonio;", 208},
	{"San Diego;", 178},
	{"San Francisco;", 146},
	{"San Jose;", 164},
	{"San José;", 226},
	{"San Juan;", 272},
	{"San Salvador;", 231},
	{"Sana'a;", 200},
	{"Santo Domingo;", 259},
	{"Sapporo;", 89},
	{"Sarajevo;", 101},
	{"Saskatoon;", 33},
	{"Seattle;", 113},
	{"Ségou;", 280},
	{"Seoul;", 125},
	{"Seville;", 192},
	{"Shanghai;", 167},
	{"Singapore;", 270},
	{"Skopje;", 124},
	{"Sochi;", 142},
	{"Sofia;", 106},
	{"Sokoto;", 280},
	{"Split;", 161},
	{"St. John's;", 50},
	{"St. Louis;", 139},
	{"Stockholm;", 66},
	{"Surabaya;", 271},
	{"Suva;", 256},
	{"Suwałki;", 72},
	{"Sydney;", 177},
	{"Tabora;", 230},
	{"Tabriz;", 126},
	{"Taipei;", 230},
	{"Tallinn;", 64},
	{"Tamale;", 279},
	{"Tamanrasset;", 217},
	{"Tampa;", 229},
	{"Tashkent;", 148},
	{"Tauranga;", 148},
	{"Tbilisi;", 129},
	{"Tegucigalpa;", 217},
	{"Tehran;", 170},
	{"Tel Aviv;", 200},
	{"Thessaloniki;", 160},
	{"Thiès;", 240},
	{"Tijuana;", 178},
	{"Timbuktu;", 280},
	{"Tirana;", 152},
	{"Toamasina;", 234},
	{"Tokyo;", 154},
	{"Toliara;", 241},
	{"Toluca;", 124},
	{"Toronto;", 94},
	{"Tripoli;", 200},
	{"Tromsø;", 29},
	{"Tucson;", 209},
	{"Tunis;", 184},
	{"Ulaanbaatar;", -04},
	{"Upington;", 204},
	{"Ürümqi;", 74},
	{"Vaduz;", 101},
	{"Valencia;", 183},
	{"Valletta;", 188},
	{"Vancouver;", 104},
	{"Veracruz;", 254},
	{"Vienna;", 104},
	{"Vientiane;", 259},
	{"Villahermosa;", 271},
	{"Vilnius;", 60},
	{"Virginia Beach;", 158},
	{"Vladivostok;", 49},
	{"Warsaw;", 85},
	{"Washington, D.C.;", 146},
	{"Wau;", 278},
	{"Wellington;", 129},
	{"Whitehorse;", -01},
	{"Wichita;", 139},
	{"Willemstad;", 280},
	{"Winnipeg;", 30},
	{"Wrocław;", 96},
	{"Xi'an;", 141},
	{"Yakutsk;", -88},
	{"Yangon;", 275},
	{"Yaoundé;", 238},
	{"Yellowknife;", -43},
	{"Yerevan;", 124},
	{"Yinchuan;", 90},
	{"Zagreb;", 107},
	{"Zanzibar City;", 260},
	{"Zürich;", 93},
}
