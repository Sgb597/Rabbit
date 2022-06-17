package Pipline.Rabbit;

import java.sql.Timestamp;
import java.util.Date;
import java.util.HashMap;

public class Tramo {
    
    private String idConductor;
    private String idVehiculo;
    private Timestamp fechaInicio;
    private Timestamp fechaFinal;
    private Double velocidad;
    private Double distancia;
	private HashMap<String, Double> tramoData;

	public Tramo() {
		this.tramoData = new HashMap<String, Double>();
    }

    public Timestamp getFechaInicio() {
        return fechaInicio;
    }
    public void setFechaInicio(Timestamp fechaInicio) {
        this.fechaInicio = fechaInicio;
    }
    public Timestamp getFechaFinal() {
        return fechaFinal;
    }
    public void setFechaFinal(Timestamp fechaFinal) {
        this.fechaFinal = fechaFinal;
    }
    public String getIdConductor() {
        return idConductor;
    }
    public void setIdConductor(String idConductor) {
        this.idConductor = idConductor;
    }
    public String getIdVehiculo() {
        return idVehiculo;
    }
    public void setIdVehiculo(String idVehiculo) {
        this.idVehiculo = idVehiculo;
    }
    public Double getVelocidad() {
        return velocidad;
    }
    public void setVelocidad(Double velocidad) {
        this.velocidad = velocidad;
    }
    public Double getDistancia() {
        return distancia;
    }
    public void setDistancia(Double distancia) {
        this.distancia = distancia;
    }
    public HashMap<String, Double> getTramoData() {
		return tramoData;
	}

	public void setTramoData(HashMap<String, Double> tramoData) {
		this.tramoData = tramoData;
	}

    void setDistance(double distanciaInicial, double distanciaFinal) {
        double deltaDistancia = distanciaFinal - distanciaInicial;
        this.distancia = deltaDistancia;
      }

    public long calculateTimeDifference() {
        // Time diff in milliseconds
        long deltaTime = this.fechaFinal.getTime() - this.fechaInicio.getTime();
        long hours = (deltaTime/ (1000 * 60 * 60)); 
    return hours;
    }

    void setVelocity() {
        double dist = this.distancia;
        long timeDiff = calculateTimeDifference();
        this.velocidad = dist / (double)timeDiff ;
    }
    
    public HashMap<String, Double> subtractTramoMaps(HashMap<String, Double> lastEvent, HashMap<String, Double> firstEvent) {
    	HashMap<String, Double> tempMap = new HashMap<String, Double>();
    	
    	for (String key: lastEvent.keySet()) {
    	    Double result = lastEvent.get(key) - firstEvent.get(key);
    	    tempMap.put(key, result);
    	}
    	return tempMap;
    }
}
