package Hdfs;
import java.io.InputStream;
import java.net.URL;
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.io.IOUtils;

public class JarDemo {
	static{
		URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
	}
	public static void main(String[] args) {
		InputStream in =null;
		try {
			in= new URL(args[0]).openStream();
			IOUtils.copyBytes(in, System.out, 4096,false);
		} catch (Exception e) {
			e.printStackTrace();
		} finally{
			IOUtils.closeStream(in);
		}
		
	}
}
