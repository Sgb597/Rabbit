package Pipline.Rabbit;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class Event {
	
	private String idEstado;
	private String idConductor;
	private String idVehiculo;
	private Date fecha;
	private Double distancia;

	public Event() {}
	
	public Event(String value) {
		int i = 0;
		for(String col: value.split(",")){
            switch(i){
                case 4: 
                    try {
                        SimpleDateFormat dateParser = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
                        // Cast String Date to Date
                        Date date = dateParser.parse(col.replaceAll("\"", ""));
                        this.fecha = date;
                    } catch(ParseException e) {
                        e.printStackTrace();
                    }
                    break;

                case 6:
                    try {
                        // Cast String Id to Integer
                        String idVehiculo = col.replaceAll("\"", "");
                        this.idVehiculo = idVehiculo;
                        //System.out.println(event.getIdVehiculo());
                    } catch(Exception e) {
                        e.printStackTrace();
                    }
                    break;

                case 7:
                    try {
                        // Cast String Id to Integer
                        String idConductor = col.replaceAll("\"", "");
                        this.idConductor = idConductor;
                    } catch(Exception e) {
                        e.printStackTrace();
                    }
                    break;

				case 11:
				try {
					// Cast String distancia to Double
					String cleanInput = col.replaceAll("\"", "");
					Double distancia = Double.parseDouble(cleanInput);
					this.distancia = distancia;
				} catch(ClassCastException e) {
					e.printStackTrace();
				}
				break;

                case 14:
                    try {
                        this.idEstado = col.replaceAll("\"", "");
                    } catch(ClassCastException e) {
                        e.printStackTrace();
                    }
                    break;
            }
            i++;
        }
	}
	
	public Date getFecha() {
		return fecha;
	}

	public void setFecha(Date fecha) {
		this.fecha = fecha;
	}

	public String getIdVehiculo() {
		return idVehiculo;
	}

	public void setIdVehiculo(String idVehiculo) {
		this.idVehiculo = idVehiculo;
	}


	public String getIdEstado() {
		return idEstado;
	}

	public void setIdEstado(String idEstado) {
		this.idEstado = idEstado;
	}

	public String getIdConductor() {
		return idConductor;
	}

	public void setIdConductor(String idConductor) {
		this.idConductor = idConductor;
	}

	public Double getDistancia() {
		return distancia;
	}

	public void setDistancia(Double distancia) {
		this.distancia = distancia;
	}
}