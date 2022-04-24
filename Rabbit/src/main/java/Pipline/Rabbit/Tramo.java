package Pipline.Rabbit;

import java.util.Date;

public class Tramo {
    
    private String idConductor;
    private String idVehiculo;
    private Date fechaInicio;
    private Date fechaFinal;
    private Double velocidad;
    private Double distancia;
    
    public Tramo() {
    }

    public Date getFechaInicio() {
        return fechaInicio;
    }
    public void setFechaInicio(Date fechaInicio) {
        this.fechaInicio = fechaInicio;
    }
    public Date getFechaFinal() {
        return fechaFinal;
    }
    public void setFechaFinal(Date fechaFinal) {
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
}
