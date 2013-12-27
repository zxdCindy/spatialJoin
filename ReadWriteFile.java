package spatialJoin;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Scanner;

/**
 * Read and write files
 * @author cindyzhang
 *
 */
public class ReadWriteFile {
	private Charset ENCODING = StandardCharsets.UTF_8;

	public Charset getENCODING() {
		return ENCODING;
	}

	public void setENCODING(Charset eNCODING) {
		ENCODING = eNCODING;
	}

	public void readFile(String fileName) throws IOException{
		Path path = Paths.get(fileName);
		try(Scanner scanner = new Scanner(path, ENCODING.name())){
			while(scanner.hasNextLine()){
				//process each line in some way
		        log(scanner.nextLine());
			}
		}
	}
	
	private static void log(Object aObject){
	    System.out.println(String.valueOf(aObject));
	}
	
	public void writeLargeTextFile(String fileName, List<String> lines) 
			throws IOException{
		//write into the file without overwriting
		FileWriter fw = new FileWriter(fileName,true);
		try (BufferedWriter writer = new BufferedWriter(fw)){
		      for(String line : lines){
		        writer.write(line);
		        writer.newLine();
		      }
		}
	}

}
