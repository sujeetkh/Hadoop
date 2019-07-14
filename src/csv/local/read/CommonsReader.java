package csv.local.read;

import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

public class CommonsReader {

	public static void main(String[] args) throws IOException {
		try (Reader reader = Files.newBufferedReader(Paths
				.get("/Users/admin/test/test_python/hadoop_in_latest/hour-12-minute-55-94-of-500-YgpBfAkjpSuq7LBh"));
				CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT);) {
			for (CSVRecord csvRecord : csvParser) {

				if (csvRecord.size() != 48) {
					System.out.println("Size : " + csvRecord.size());
					System.out.println("Record:-  " + csvRecord.toString());
				}

			}
		}

	}
}
