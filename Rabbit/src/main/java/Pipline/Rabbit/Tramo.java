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

    void setDistance(double lat1, double lon1, double lat2, double lon2) {
        double theta = lon1 - lon2;
        double dist = Math.sin(deg2rad(lat1)) * Math.sin(deg2rad(lat2)) + Math.cos(deg2rad(lat1)) * Math.cos(deg2rad(lat2)) * Math.cos(deg2rad(theta));
        dist = Math.acos(dist);
        dist = rad2deg(dist);
        dist = dist * 60 * 1.1515;
        // Distancia en kilometros
        dist = dist * 1.609344;
        
        this.distancia = dist;
      }
      
      // Grados a radianes
      private double deg2rad(double deg) {
        return (deg * Math.PI / 180.0);
      }
      
      // Radianes a grados
      private double rad2deg(double rad) {
        return (rad * 180.0 / Math.PI);
      }

      private double calculateTimeDifference() {
        double hours = this.fechaInicio.getTime() - this.fechaFinal.getTime();
        return hours;
      }

      void setVelocity() {
          double dist = this.distancia;
          double timeDiff = calculateTimeDifference();
          this.velocidad = dist / timeDiff;
      }
}
