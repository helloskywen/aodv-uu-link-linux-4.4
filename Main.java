import java.io.*;
import java.util.*;
import java.util.regex.Pattern;

import com.mysql.jdbc.DatabaseMetaData;

import java.net.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
public class Main {	
	private static final int SERVER_PORT = 9000;	
	private static final int BUF_SIZE = 64;		
	
	public static float SHT2x_CalcTemperatureC(int temperature){
		float temperatureC;
	    temperature &= ~0x0003;
	    temperatureC = (float) (-46.85 + 175.72/65536 * (float)temperature);	    
		return temperatureC;
	}
	
	public static float SHT2x_CalcRH(int humidity){
	    float humidityRH;
	    humidity &= ~0x0003;
	    humidityRH = (float) (-6.0 + 125.0/65536 * (float)humidity);	    
		return humidityRH;
	}
	
	public static boolean isInteger(String str) {    
	    Pattern pattern = Pattern.compile("^[-\\+]?[\\d]*$");    
	    return pattern.matcher(str).matches();    
	}  

	public static void main(String[] args) throws IOException, ClassNotFoundException, SQLException{
		// TODO Auto-generated method stub
		Map<RoomLocation, String> RoomWithIpMap = new HashMap<RoomLocation, String>();
		byte[] buf = new byte[BUF_SIZE];
		
		DatagramSocket server_socket = new DatagramSocket(SERVER_PORT);
		DatagramPacket packet = new DatagramPacket(buf, BUF_SIZE);
		
		RoomLocation room_location = null;		
		
		String sql = null;
		
		Class.forName("com.mysql.jdbc.Driver");
		String url = "jdbc:mysql://localhost:3306/sensor?useUnicode=true&characterEncoding=UTF-8";
		String username = "root";
		String password = "";
		Connection conn = DriverManager.getConnection(url,username,password);		
		if(conn !=null){
			System.out.println("Connect mysql success!\n");			
		} else{
			System.out.println("Connect mysql failed!\n");
			System.exit(0);
		}		
		Statement stat = conn.createStatement();
		ResultSet rs = null;
		
		sql="create table t_sensor(time DateTime not null, building_number int not null, building_level int not null, room int not null, device_type varchar(20) not null, "
				+ "device_number int not null, data_type varchar(16) not null, value float, primary key(time,building_number,building_level,room,device_type,device_number,data_type)) DEFAULT   CHARSET=utf8";
		
		DatabaseMetaData meta = (DatabaseMetaData) conn.getMetaData();
		rs = meta.getTables(null, null, "t_sensor", null);
		if(rs.next() == false)
			stat.execute(sql);	
		
		String sql_device_type;
		String sql_data_type;
		
		new RecvControlCmd(RoomWithIpMap);		
		
		while(true){
			server_socket.receive(packet);			
			//InetAddress inetAddress = packet.getAddress();
			byte[] data = packet.getData();
			String data_str = new String(data);
			String array[] = data_str.split(",");			
			
			if(!isInteger(array[0])){
				continue;
			}			
			
			long time = Long.parseLong(array[0]);			
			Date date = new Date(time * 1000);			
			SimpleDateFormat sdf = null;
			sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			String dateTime = sdf.format(date);	
			
			System.out.print(array[0] + ": ");			
			System.out.println(dateTime);			
			
			int building_number = Integer.parseInt(array[1]);
			int building_level = Integer.parseInt(array[2]);
			int room = Integer.parseInt(array[3]);
			int dev_type = Integer.parseInt(array[4]);
			int dev_number = Integer.parseInt(array[5]);
			int data_type = Integer.parseInt(array[6]);
			
			String room_ip = array[11].trim();				
			room_location = new RoomLocation(building_number, building_level, room);
			RoomWithIpMap.put(room_location, room_ip);
			
			int[] d = new int[4];
			d[0] = Integer.parseInt(array[7]);
			d[1] = Integer.parseInt(array[8]);
			d[2] = Integer.parseInt(array[9]);
			d[3] = Integer.parseInt(array[10]);	
			
			if(data_type == 0x0A){//sensor measure value
				if(dev_type == 0x94){
					sql_device_type = "温度湿度照度传感器";
					if(d[2] !=0 && d[3] !=0){
						
						int temperature = d[0]<<8 | d[1];
						int humidity = d[2]<<8 | d[3];
						
						float temperatureC = SHT2x_CalcTemperatureC(temperature);
						float humidityRH = SHT2x_CalcRH(humidity);	
						
						System.out.println(room_location.building_number + "-" + room_location.building_level + "-" + room_location.room + " " + room_ip);
						System.out.println(dateTime + ", " + building_number + ", " + building_level + ", " + room + ", " + dev_type + ", " + dev_number + ", T(℃): " + temperatureC + ", RH: " + humidityRH);
						System.out.println();
						
						sql_data_type = "temperatureC";	
						sql = "insert into t_sensor values(" + "\"" + dateTime + "\"" + "," + "\"" + building_number + "\"" + "," + "\"" + building_level + "\"" + "," + "\"" + room + "\"" + "," + "\"" + sql_device_type + "\"" + "," + "\"" + dev_number + "\"" + "," + "\"" + sql_data_type + "\"" + "," + "\"" + temperatureC + "\"" + ")"; 
						stat.executeUpdate(sql);
						
						sql_data_type = "humidityRH";	
						sql = "insert into t_sensor values(" + "\"" + dateTime + "\"" + "," + "\"" + building_number + "\"" + "," + "\"" + building_level + "\"" + "," + "\"" + room + "\"" + "," + "\"" + sql_device_type + "\"" + "," + "\"" + dev_number + "\"" + "," + "\"" + sql_data_type + "\"" + "," + "\"" + humidityRH + "\"" + ")"; 
						stat.executeUpdate(sql);						
					}
				}				
			} else if(data_type == 0xff){
				int value = d[0];
				sql_data_type = "status";
				if(dev_type == 0xd0){
					sql_device_type = "灯控制器";						
					sql = "insert into t_sensor values(" + "\"" + dateTime +"\"" + "," + "\"" + building_number + "\"" + "," + "\"" + building_level + "\"" + "," + "\"" + room + "\"" + "," + "\"" + sql_device_type + "\"" + "," + "\"" + dev_number + "\"" + "," + "\"" + sql_data_type + "\"" + ","  + "\"" + value + "\"" + ")";
					stat.executeUpdate(sql);					
				} else if(dev_type == 0xcc){
					sql_device_type = "FCU控制器";						
					sql = "insert into t_sensor values(" + "\"" +dateTime +"\"" + ", " + "\"" + building_number + "\"" + "," + "\"" + building_level + "\"" + "," + "\"" + room + "\"" + "," + "\"" + sql_device_type + "\"" + "," + "\"" + dev_number + "\"" + "," + "\"" + sql_data_type + "\"" + ","  + "\"" + value + "\"" + ")";
					stat.executeUpdate(sql);
				} else if(dev_type == 0xd4){
					sql_device_type = "窗帘控制器";					
					sql = "insert into t_sensor values(" + "\"" +dateTime +"\"" + ", " + "\"" + building_number + "\"" + "," + "\"" + building_level + "\"" + "," + "\"" + room + "\"" + "," + "\"" + sql_device_type + "\"" + "," + "\"" + dev_number + "\"" + "," + "\"" + sql_data_type + "\"" + ","  + "\"" + value + "\"" + ")";
					stat.executeUpdate(sql);	
				}
			}				
		}	
	}
}