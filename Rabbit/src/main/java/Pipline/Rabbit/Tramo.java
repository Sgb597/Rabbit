package Pipline.Rabbit;

import java.sql.Timestamp;
import java.util.Date;

public class Tramo {
    
    private String idConductor;
    private String idVehiculo;
    private Timestamp fechaInicio;
    private Timestamp fechaFinal;
    private Double velocidad;
    private Double distancia;
    private String cruiseActive;
	private String rpmExcesivas;
	private String frenadasBruscas;
	private String aceleracionesBruscas;
	private String cNoPredictiva2;
	private String zRoja2;
	private String zMasVerde2;
	private String frenadasBruscas2;
	private String aceleracionesBruscas2;
	private String ralInec2;
	private String tiempoConduccionCrucero2;
	private String metrosAscendidos2;
	private String metrosDescendidos2;
	private String odometro2;
	private String totalFuel2;
	private String tiempoRal2;
	private String consumoRal2;
	private String tiempoConduccion2;
	private String nFreno3;
	private String nEmbrague3;
	private String tiempoMotor3;
	
    public String getCruiseActive() {
		return cruiseActive;
	}

	public void setCruiseActive(String cruiseActive) {
		this.cruiseActive = cruiseActive;
	}

	public String getRpmExcesivas() {
		return rpmExcesivas;
	}

	public void setRpmExcesivas(String rpmExcesivas) {
		this.rpmExcesivas = rpmExcesivas;
	}

	public String getFrenadasBruscas() {
		return frenadasBruscas;
	}

	public void setFrenadasBruscas(String frenadasBruscas) {
		this.frenadasBruscas = frenadasBruscas;
	}

	public String getAceleracionesBruscas() {
		return aceleracionesBruscas;
	}

	public void setAceleracionesBruscas(String aceleracionesBruscas) {
		this.aceleracionesBruscas = aceleracionesBruscas;
	}

	public String getcNoPredictiva2() {
		return cNoPredictiva2;
	}

	public void setcNoPredictiva2(String cNoPredictiva2) {
		this.cNoPredictiva2 = cNoPredictiva2;
	}

	public String getzRoja2() {
		return zRoja2;
	}

	public void setzRoja2(String zRoja2) {
		this.zRoja2 = zRoja2;
	}

	public String getzMasVerde2() {
		return zMasVerde2;
	}

	public void setzMasVerde2(String zMasVerde2) {
		this.zMasVerde2 = zMasVerde2;
	}

	public String getFrenadasBruscas2() {
		return frenadasBruscas2;
	}

	public void setFrenadasBruscas2(String frenadasBruscas2) {
		this.frenadasBruscas2 = frenadasBruscas2;
	}

	public String getAceleracionesBruscas2() {
		return aceleracionesBruscas2;
	}

	public void setAceleracionesBruscas2(String aceleracionesBruscas2) {
		this.aceleracionesBruscas2 = aceleracionesBruscas2;
	}

	public String getRalInec2() {
		return ralInec2;
	}

	public void setRalInec2(String ralInec2) {
		this.ralInec2 = ralInec2;
	}

	public String getTiempoConduccionCrucero2() {
		return tiempoConduccionCrucero2;
	}

	public void setTiempoConduccionCrucero2(String tiempoConduccionCrucero2) {
		this.tiempoConduccionCrucero2 = tiempoConduccionCrucero2;
	}

	public String getMetrosAscendidos2() {
		return metrosAscendidos2;
	}

	public void setMetrosAscendidos2(String metrosAscendidos2) {
		this.metrosAscendidos2 = metrosAscendidos2;
	}

	public String getMetrosDescendidos2() {
		return metrosDescendidos2;
	}

	public void setMetrosDescendidos2(String metrosDescendidos2) {
		this.metrosDescendidos2 = metrosDescendidos2;
	}

	public String getOdometro2() {
		return odometro2;
	}

	public void setOdometro2(String odometro2) {
		this.odometro2 = odometro2;
	}

	public String getTotalFuel2() {
		return totalFuel2;
	}

	public void setTotalFuel2(String totalFuel2) {
		this.totalFuel2 = totalFuel2;
	}

	public String getTiempoRal2() {
		return tiempoRal2;
	}

	public void setTiempoRal2(String tiempoRal2) {
		this.tiempoRal2 = tiempoRal2;
	}

	public String getConsumoRal2() {
		return consumoRal2;
	}

	public void setConsumoRal2(String consumoRal2) {
		this.consumoRal2 = consumoRal2;
	}

	public String getTiempoConduccion2() {
		return tiempoConduccion2;
	}

	public void setTiempoConduccion2(String tiempoConduccion2) {
		this.tiempoConduccion2 = tiempoConduccion2;
	}

	public String getnFreno3() {
		return nFreno3;
	}

	public void setnFreno3(String nFreno3) {
		this.nFreno3 = nFreno3;
	}

	public String getnEmbrague3() {
		return nEmbrague3;
	}

	public void setnEmbrague3(String nEmbrague3) {
		this.nEmbrague3 = nEmbrague3;
	}

	public String getTiempoMotor3() {
		return tiempoMotor3;
	}

	public void setTiempoMotor3(String tiempoMotor3) {
		this.tiempoMotor3 = tiempoMotor3;
	}

	public Tramo() {
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
