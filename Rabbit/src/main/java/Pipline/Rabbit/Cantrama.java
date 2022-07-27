package Pipline.Rabbit;

import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class Cantrama {
	
	private String idCantrama;
	private Timestamp fecha;
	private String idVehiculo;
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
	
	public String getIdCantrama() {
		return idCantrama;
	}

	public void setIdCantrama(String idCantrama) {
		this.idCantrama = idCantrama;
	}

	public String getIdVehiculo() {
		return idVehiculo;
	}

	public void setIdVehiculo(String idVehiculo) {
		this.idVehiculo = idVehiculo;
	}
	
	public Timestamp getFecha() {
		return fecha;
	}

	public void setFecha(Timestamp fecha) {
		this.fecha = fecha;
	}

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

	public Cantrama(String value) {
		int i = 0;
		for(String col: value.split(",")){
            switch(i){
                case 0: 
                	try {
                        String idCantrama = col.replaceAll("\"", "");
                        this.idCantrama = idCantrama;
                    } catch(Exception e) {
                        e.printStackTrace();
                    }
                    break;

                case 1:
                    try {
                        String idVehiculo = col.replaceAll("\"", "");
                        this.idVehiculo = idVehiculo;
                    } catch(Exception e) {
                        e.printStackTrace();
                    }
                    break;
                case 2: 
                    try {
                        SimpleDateFormat dateParser = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
                        String dateAsString = col.replaceAll("\"", "");
                        Date date = dateParser.parse(dateAsString);
                        Timestamp timestamp = new java.sql.Timestamp(date.getTime());
                        this.fecha = timestamp;
                    } catch(ParseException e) {
                        e.printStackTrace();
                    }
                    break;
                case 6:
                    try {
                        String cruiseActive = col.replaceAll("\"", "");
                        this.cruiseActive = cruiseActive;
                    } catch(Exception e) {
                        e.printStackTrace();
                    }
                    break;
                case 12:
                    try {
                        String rpmExcesivas = col.replaceAll("\"", "");
                        this.rpmExcesivas = rpmExcesivas;
                    } catch(Exception e) {
                        e.printStackTrace();
                    }
                    break;
                case 13:
                    try {
                        String frenadasBruscas = col.replaceAll("\"", "");
                        this.frenadasBruscas = frenadasBruscas;
                    } catch(Exception e) {
                        e.printStackTrace();
                    }
                    break;
                case 14:
                    try {
                        String aceleracionesBruscas = col.replaceAll("\"", "");
                        this.aceleracionesBruscas = aceleracionesBruscas;
                    } catch(Exception e) {
                        e.printStackTrace();
                    }
                    break;
                case 21:
                    try {
                        String cNoPredictiva2 = col.replaceAll("\"", "");
                        this.cNoPredictiva2 = cNoPredictiva2;
                    } catch(Exception e) {
                        e.printStackTrace();
                    }
                    break;
                case 22:
                    try {
                        String zRoja2 = col.replaceAll("\"", "");
                        this.zRoja2 = zRoja2;
                    } catch(Exception e) {
                        e.printStackTrace();
                    }
                    break;
                case 23:
                    try {
                        String zMasVerde2 = col.replaceAll("\"", "");
                        this.zMasVerde2 = zMasVerde2;
                    } catch(Exception e) {
                        e.printStackTrace();
                    }
                    break;
                case 24:
                    try {
                        String frenadasBruscas2 = col.replaceAll("\"", "");
                        this.frenadasBruscas2 = frenadasBruscas2;
                    } catch(Exception e) {
                        e.printStackTrace();
                    }
                    break;
                case 25:
                    try {
                        String aceleracionesBruscas2 = col.replaceAll("\"", "");
                        this.aceleracionesBruscas2 = aceleracionesBruscas2;
                    } catch(Exception e) {
                        e.printStackTrace();
                    }
                    break;
                case 28:
                    try {
                        String ralInnec2 = col.replaceAll("\"", "");
                        this.ralInec2 = ralInnec2;
                    } catch(Exception e) {
                        e.printStackTrace();
                    }
                    break;
                case 35:
                    try {
                        String tiempoConduccionCrucero2 = col.replaceAll("\"", "");
                        this.tiempoConduccionCrucero2 = tiempoConduccionCrucero2;
                    } catch(Exception e) {
                        e.printStackTrace();
                    }
                    break;
                case 37:
                    try {
                        String metrosAscendidos2 = col.replaceAll("\"", "");
                        this.metrosAscendidos2 = metrosAscendidos2;
                    } catch(Exception e) {
                        e.printStackTrace();
                    }
                    break;
                case 38:
                    try {
                        String metrosDescendidos2 = col.replaceAll("\"", "");
                        this.metrosDescendidos2 = metrosDescendidos2;
                    } catch(Exception e) {
                        e.printStackTrace();
                    }
                    break;
                case 40:
                    try {
                        String odometro2 = col.replaceAll("\"", "");
                        this.odometro2 = odometro2;
                    } catch(Exception e) {
                        e.printStackTrace();
                    }
                    break;
                case 41:
                    try {
                        String totalFuel2 = col.replaceAll("\"", "");
                        this.totalFuel2 = totalFuel2;
                    } catch(Exception e) {
                        e.printStackTrace();
                    }
                    break;
                case 42:
                    try {
                        String tiempoRal2 = col.replaceAll("\"", "");
                        this.tiempoRal2 = tiempoRal2;
                    } catch(Exception e) {
                        e.printStackTrace();
                    }
                    break;
                case 43:
                    try {
                        String consumoRal2 = col.replaceAll("\"", "");
                        this.consumoRal2 = consumoRal2;
                    } catch(Exception e) {
                        e.printStackTrace();
                    }
                    break;
                case 47:
                    try {
                        String tiempoConduccion2 = col.replaceAll("\"", "");
                        this.tiempoConduccion2 = tiempoConduccion2;
                    } catch(Exception e) {
                        e.printStackTrace();
                    }
                    break;
                case 49:
                    try {
                        String nFreno3 = col.replaceAll("\"", "");
                        this.nFreno3 = nFreno3;
                    } catch(Exception e) {
                        e.printStackTrace();
                    }
                    break;
                case 50:
                    try {
                        String nEmbrague3 = col.replaceAll("\"", "");
                        this.nEmbrague3 = nEmbrague3;
                    } catch(Exception e) {
                        e.printStackTrace();
                    }
                    break;
                case 54:
                    try {
                        String tiempoMotor3 = col.replaceAll("\"", "");
                        this.tiempoMotor3 = tiempoMotor3;
                    } catch(Exception e) {
                        e.printStackTrace();
                    }
                    break;
            }
            i++;
        }
	}
	
	
}