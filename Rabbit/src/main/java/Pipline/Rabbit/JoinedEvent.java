package Pipline.Rabbit;

import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;

public class JoinedEvent {
	
	private String idEstado;
	private String idConductor;
	private String idVehiculo;
	private Timestamp fecha;
	private Double distancia;
	private Double cruiseActive;
	private Double rpmExcesivas;
	private Double frenadasBruscas;
	private Double aceleracionesBruscas;
	private Double cNoPredictiva2;
	private Double zRoja2;
	private Double zMasVerde2;
	private Double frenadasBruscas2;
	private Double aceleracionesBruscas2;
	private Double ralInec2;
	private Double tiempoConduccionCrucero2;
	private Double metrosAscendidos2;
	private Double metrosDescendidos2;
	private Double odometro2;
	private Double totalFuel2;
	private Double tiempoRal2;
	private Double consumoRal2;
	private Double tiempoConduccion2;
	private Double nFreno3;
	private Double nEmbrague3;
	private Double tiempoMotor3;
	private HashMap<String, Double> tramoData;

	public JoinedEvent() {}
	
	public JoinedEvent(String value) {
		int i = 0;
		for(String col: value.split(",")){
            switch(i){
            case 0:
                try {
                    String idVehiculo = col.replaceAll("\"", "");
                    this.idVehiculo = idVehiculo;
                } catch(Exception e) {
                    e.printStackTrace();
                }
                break;
            case 1:
                try {
                    String idConductor = col.replaceAll("\"", "");
                    this.idConductor = idConductor;
                } catch(Exception e) {
                    e.printStackTrace();
                }
                break;
            case 2:
                try {
                    String idEstado = col.replaceAll("\"", "");
                    this.idEstado = idEstado;
                } catch(Exception e) {
                    e.printStackTrace();
                }
                break;
            case 3: 
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
            case 4:
            	if(!isNull(col.replaceAll("\"", ""))) {
            		try {
    					// Cast String distancia to Double
    					String cleanInput = col.replaceAll("\"", "");
    					Double distancia = Double.parseDouble(cleanInput);
    					this.distancia = distancia;
    				} catch(ClassCastException e) {
    					e.printStackTrace();
    				}
            	}
            	else {
            		this.distancia = 0.0;
            	}
				break;
                case 5:
                	if(!isNull(col.replaceAll("\"", ""))) {
                		try {
                			String cleanInput = col.replaceAll("\"", "");
        					Double cruiseActive = Double.parseDouble(cleanInput);
                            this.cruiseActive = cruiseActive;
                            this.tramoData.put("cruiseActive", cruiseActive);
                        } catch(Exception e) {
                            e.printStackTrace();
                        }
                	} else {
                    	this.cruiseActive = 0.0;
                    	this.tramoData.put("cruiseActive", 0.0);
                    }
                    break;
                case 6:
                	if(!isNull(col.replaceAll("\"", ""))) {
                		try {
                			String cleanInput = col.replaceAll("\"", "");
        					Double rpmExcesivas = Double.parseDouble(cleanInput);
                            this.rpmExcesivas = rpmExcesivas;
                            this.tramoData.put("rpmExcesivas", rpmExcesivas);
                        } catch(Exception e) {
                            e.printStackTrace();
                        }
                	} else {
                    	this.rpmExcesivas = 0.0;
                    	this.tramoData.put("rpmExcesivas", 0.0);

                    }
                    break;
                case 7:
                	if(!isNull(col.replaceAll("\"", ""))) {
                		try {
                			String cleanInput = col.replaceAll("\"", "");
        					Double frenadasBruscas = Double.parseDouble(cleanInput);
                            this.frenadasBruscas = frenadasBruscas;
                            this.tramoData.put("frenadasBruscas", frenadasBruscas);
                        } catch(Exception e) {
                            e.printStackTrace();
                        }
                	} else {
                    	this.frenadasBruscas = 0.0;
                    	this.tramoData.put("frenadasBruscas", 0.0);
                    }
                    break;
                case 8:
                	if(!isNull(col.replaceAll("\"", ""))) {
                		try {
                			String cleanInput = col.replaceAll("\"", "");
        					Double aceleracionesBruscas = Double.parseDouble(cleanInput);
                            this.aceleracionesBruscas = aceleracionesBruscas;
                            this.tramoData.put("aceleracionesBruscas", aceleracionesBruscas);
                        } catch(Exception e) {
                            e.printStackTrace();
                        }
                	} else {
                    	this.aceleracionesBruscas = 0.0;
                    	this.tramoData.put("aceleracionesBruscas", 0.0);

                    }
                    break;
                case 9:
                	if(!isNull(col.replaceAll("\"", ""))) {
                		try {
                			String cleanInput = col.replaceAll("\"", "");
        					Double cNoPredictiva2 = Double.parseDouble(cleanInput);
                            this.cNoPredictiva2 = cNoPredictiva2;
                            this.tramoData.put("cNoPredictiva2", cNoPredictiva2);
                        } catch(Exception e) {
                            e.printStackTrace();
                        }
                	} else {
                    	this.cNoPredictiva2 = 0.0;
                        this.tramoData.put("cNoPredictiva2", 0.0);
                    }
                    break;
                case 10:
                	if(!isNull(col.replaceAll("\"", ""))) {
                		try {
                			String cleanInput = col.replaceAll("\"", "");
        					Double zRoja2 = Double.parseDouble(cleanInput);
                            this.zRoja2 = zRoja2;
                            this.tramoData.put("zRoja2", zRoja2);
                        } catch(Exception e) {
                            e.printStackTrace();
                        }
                	} else {
                    	this.zRoja2 = 0.0;
                        this.tramoData.put("zRoja2", 0.0);
                    }
                    break;
                case 11:
                	if(!isNull(col.replaceAll("\"", ""))) {
                		try {
                			String cleanInput = col.replaceAll("\"", "");
        					Double zMasVerde2 = Double.parseDouble(cleanInput);
                            this.zMasVerde2 = zMasVerde2;
                            this.tramoData.put("zMasVerde2", zMasVerde2);
                        } catch(Exception e) {
                            e.printStackTrace();
                        }
                	} else {
                    	this.zMasVerde2 = 0.0;
                        this.tramoData.put("zMasVerde2", 0.0);
                    }
                    break;
                case 12:
                	if(!isNull(col.replaceAll("\"", ""))) {
                		try {
                			String cleanInput = col.replaceAll("\"", "");
        					Double frenadasBruscas2 = Double.parseDouble(cleanInput);
                            this.frenadasBruscas2 = frenadasBruscas2;
                            this.tramoData.put("frenadasBruscas2", frenadasBruscas2);
                        } catch(Exception e) {
                            e.printStackTrace();
                        }
                	} else {
                    	this.frenadasBruscas2 = 0.0;
                        this.tramoData.put("frenadasBruscas2", 0.0);
                    }
                    break;
                case 13:
                	if(!isNull(col.replaceAll("\"", ""))) {
                		try {
                			String cleanInput = col.replaceAll("\"", "");
        					Double aceleracionesBruscas2 = Double.parseDouble(cleanInput);
                            this.aceleracionesBruscas2 = aceleracionesBruscas2;
                            this.tramoData.put("aceleracionesBruscas2", aceleracionesBruscas2);
                        } catch(Exception e) {
                            e.printStackTrace();
                        }
                	} else {
                    	this.aceleracionesBruscas2 = 0.0;
                        this.tramoData.put("aceleracionesBruscas2", 0.0);
                    }
                    break;
                case 14:
                   	if(!isNull(col.replaceAll("\"", ""))) {
                		try {
                			String cleanInput = col.replaceAll("\"", "");
        					Double ralInnec2 = Double.parseDouble(cleanInput);
                            this.ralInec2 = ralInnec2;
                            this.tramoData.put("ralInnec2", ralInnec2);
                        } catch(Exception e) {
                            e.printStackTrace();
                        }
                	} else {
                    	this.ralInec2 = 0.0;
                        this.tramoData.put("ralInnec2", 0.0);
                    }
                    break;
                case 15:
                   	if(!isNull(col.replaceAll("\"", ""))) {
                		try {
                			String cleanInput = col.replaceAll("\"", "");
        					Double tiempoConduccionCrucero2 = Double.parseDouble(cleanInput);
                            this.tiempoConduccionCrucero2 = tiempoConduccionCrucero2;
                            this.tramoData.put("tiempoConduccionCrucero2", tiempoConduccionCrucero2);
                        } catch(Exception e) {
                            e.printStackTrace();
                        }
                	} else {
                    	this.tiempoConduccionCrucero2 = 0.0;
                        this.tramoData.put("tiempoConduccionCrucero2", 0.0);
                    }
                    break;
                case 16:
                   	if(!isNull(col.replaceAll("\"", ""))) {
                		try {
                			String cleanInput = col.replaceAll("\"", "");
        					Double metrosAscendidos2 = Double.parseDouble(cleanInput);
                            this.metrosAscendidos2 = metrosAscendidos2;
                            this.tramoData.put("metrosAscendidos2", metrosAscendidos2);
                        } catch(Exception e) {
                            e.printStackTrace();
                        }
                	} else {
                    	this.metrosAscendidos2 = 0.0;
                        this.tramoData.put("metrosAscendidos2", 0.0);
                    }
                    break;
                case 17:
                   	if(!isNull(col.replaceAll("\"", ""))) {
                		try {
                			String cleanInput = col.replaceAll("\"", "");
        					Double metrosDescendidos2 = Double.parseDouble(cleanInput);
                            this.metrosDescendidos2 = metrosDescendidos2;
                            this.tramoData.put("metrosDescendidos2", metrosDescendidos2);
                        } catch(Exception e) {
                            e.printStackTrace();
                        }
                	} else {
                    	this.metrosDescendidos2 = 0.0;
                        this.tramoData.put("metrosDescendidos2", 0.0);
                    }
                    break;
                case 18:
                   	if(!isNull(col.replaceAll("\"", ""))) {
                		try {
                			String cleanInput = col.replaceAll("\"", "");
        					Double odometro2 = Double.parseDouble(cleanInput);
                            this.odometro2 = odometro2;
                            this.tramoData.put("odometro2", odometro2);
                        } catch(Exception e) {
                            e.printStackTrace();
                        }
                	} else {
                    	this.odometro2 = 0.0;
                        this.tramoData.put("odometro2", 0.0);
                    }
                    break;
                case 19:
                   	if(!isNull(col.replaceAll("\"", ""))) {
                		try {
                			String cleanInput = col.replaceAll("\"", "");
        					Double totalFuel2 = Double.parseDouble(cleanInput);
                            this.totalFuel2 = totalFuel2;
                            this.tramoData.put("totalFuel2", totalFuel2);
                        } catch(Exception e) {
                            e.printStackTrace();
                        }
                	} else {
                    	this.totalFuel2 = 0.0;
                        this.tramoData.put("totalFuel2", 0.0);
                    }
                    break;
                case 20:
                   	if(!isNull(col.replaceAll("\"", ""))) {
                		try {
                			String cleanInput = col.replaceAll("\"", "");
        					Double tiempoRal2 = Double.parseDouble(cleanInput);
                            this.tiempoRal2 = tiempoRal2;
                            this.tramoData.put("tiempoRal2", tiempoRal2);
                        } catch(Exception e) {
                            e.printStackTrace();
                        }
                	} else {
                    	this.tiempoRal2 = 0.0;
                        this.tramoData.put("tiempoRal2", 0.0);
                    }
                    break;
                case 21:
                   	if(!isNull(col.replaceAll("\"", ""))) {
                		try {
                			String cleanInput = col.replaceAll("\"", "");
        					Double consumoRal2 = Double.parseDouble(cleanInput);
                            this.consumoRal2 = consumoRal2;
                            this.tramoData.put("consumoRal2", consumoRal2);
                        } catch(Exception e) {
                            e.printStackTrace();
                        }
                	} else {
                    	this.consumoRal2 = 0.0;
                        this.tramoData.put("consumoRal2", 0.0);
                    }
                    break;
                case 22:
                   	if(!isNull(col.replaceAll("\"", ""))) {
                		try {
                			String cleanInput = col.replaceAll("\"", "");
        					Double tiempoConduccion2 = Double.parseDouble(cleanInput);
                            this.tiempoConduccion2 = tiempoConduccion2;
                            this.tramoData.put("tiempoConduccion2", tiempoConduccion2);
                        } catch(Exception e) {
                            e.printStackTrace();
                        }
                	} else {
                    	this.tiempoConduccion2 = 0.0;
                        this.tramoData.put("tiempoConduccion2", 0.0);
                    }
                    break;
                case 23:
                   	if(!isNull(col.replaceAll("\"", ""))) {
                		try {
                			String cleanInput = col.replaceAll("\"", "");
        					Double nFreno3 = Double.parseDouble(cleanInput);
                            this.nFreno3 = nFreno3;
                            this.tramoData.put("nFreno3", nFreno3);
                        } catch(Exception e) {
                            e.printStackTrace();
                        }
                	} else {
                    	this.nFreno3 = 0.0;
                        this.tramoData.put("nFreno3", 0.0);
                    }
                    break;
                case 24:
                   	if(!isNull(col.replaceAll("\"", ""))) {
                		try {
                			String cleanInput = col.replaceAll("\"", "");
        					Double nEmbrague3 = Double.parseDouble(cleanInput);
                            this.nEmbrague3 = nEmbrague3;
                            this.tramoData.put("nEmbrague3", nEmbrague3);
                        } catch(Exception e) {
                            e.printStackTrace();
                        }
                	} else {
                    	this.nEmbrague3 = 0.0;
                        this.tramoData.put("nEmbrague3", 0.0);
                    }
                    break;
                case 25:
                   	if(!isNull(col.replaceAll("\"", ""))) {
                		try {
                			String cleanInput = col.replaceAll("\"", "");
        					Double tiempoMotor3 = Double.parseDouble(cleanInput);
                            this.tiempoMotor3 = tiempoMotor3;
                            this.tramoData.put("tiempoMotor3", tiempoMotor3);
                        } catch(Exception e) {
                            e.printStackTrace();
                        }
                	} else {
                    	this.tiempoMotor3 = 0.0;
                        this.tramoData.put("tiempoMotor3", 0.0);
                    }
                    break;
            }
            i++;
        }
	}
	
	public Timestamp getFecha() {
		return fecha;
	}

	public void setFecha(Timestamp fecha) {
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
	
	public Double getCruiseActive() {
		return cruiseActive;
	}

	public void setCruiseActive(Double cruiseActive) {
		this.cruiseActive = cruiseActive;
	}

	public Double getRpmExcesivas() {
		return rpmExcesivas;
	}

	public void setRpmExcesivas(Double rpmExcesivas) {
		this.rpmExcesivas = rpmExcesivas;
	}

	public Double getFrenadasBruscas() {
		return frenadasBruscas;
	}

	public void setFrenadasBruscas(Double frenadasBruscas) {
		this.frenadasBruscas = frenadasBruscas;
	}

	public Double getAceleracionesBruscas() {
		return aceleracionesBruscas;
	}

	public void setAceleracionesBruscas(Double aceleracionesBruscas) {
		this.aceleracionesBruscas = aceleracionesBruscas;
	}

	public Double getcNoPredictiva2() {
		return cNoPredictiva2;
	}

	public void setcNoPredictiva2(Double cNoPredictiva2) {
		this.cNoPredictiva2 = cNoPredictiva2;
	}

	public Double getzRoja2() {
		return zRoja2;
	}

	public void setzRoja2(Double zRoja2) {
		this.zRoja2 = zRoja2;
	}

	public Double getzMasVerde2() {
		return zMasVerde2;
	}

	public void setzMasVerde2(Double zMasVerde2) {
		this.zMasVerde2 = zMasVerde2;
	}

	public Double getFrenadasBruscas2() {
		return frenadasBruscas2;
	}

	public void setFrenadasBruscas2(Double frenadasBruscas2) {
		this.frenadasBruscas2 = frenadasBruscas2;
	}

	public Double getAceleracionesBruscas2() {
		return aceleracionesBruscas2;
	}

	public void setAceleracionesBruscas2(Double aceleracionesBruscas2) {
		this.aceleracionesBruscas2 = aceleracionesBruscas2;
	}

	public Double getRalInec2() {
		return ralInec2;
	}

	public void setRalInec2(Double ralInec2) {
		this.ralInec2 = ralInec2;
	}

	public Double getTiempoConduccionCrucero2() {
		return tiempoConduccionCrucero2;
	}

	public void setTiempoConduccionCrucero2(Double tiempoConduccionCrucero2) {
		this.tiempoConduccionCrucero2 = tiempoConduccionCrucero2;
	}

	public Double getMetrosAscendidos2() {
		return metrosAscendidos2;
	}

	public void setMetrosAscendidos2(Double metrosAscendidos2) {
		this.metrosAscendidos2 = metrosAscendidos2;
	}

	public Double getMetrosDescendidos2() {
		return metrosDescendidos2;
	}

	public void setMetrosDescendidos2(Double metrosDescendidos2) {
		this.metrosDescendidos2 = metrosDescendidos2;
	}

	public Double getOdometro2() {
		return odometro2;
	}

	public void setOdometro2(Double odometro2) {
		this.odometro2 = odometro2;
	}

	public Double getTotalFuel2() {
		return totalFuel2;
	}

	public void setTotalFuel2(Double totalFuel2) {
		this.totalFuel2 = totalFuel2;
	}

	public Double getTiempoRal2() {
		return tiempoRal2;
	}

	public void setTiempoRal2(Double tiempoRal2) {
		this.tiempoRal2 = tiempoRal2;
	}

	public Double getConsumoRal2() {
		return consumoRal2;
	}

	public void setConsumoRal2(Double consumoRal2) {
		this.consumoRal2 = consumoRal2;
	}

	public Double getTiempoConduccion2() {
		return tiempoConduccion2;
	}

	public void setTiempoConduccion2(Double tiempoConduccion2) {
		this.tiempoConduccion2 = tiempoConduccion2;
	}

	public Double getnFreno3() {
		return nFreno3;
	}

	public void setnFreno3(Double nFreno3) {
		this.nFreno3 = nFreno3;
	}

	public Double getnEmbrague3() {
		return nEmbrague3;
	}

	public void setnEmbrague3(Double nEmbrague3) {
		this.nEmbrague3 = nEmbrague3;
	}

	public Double getTiempoMotor3() {
		return tiempoMotor3;
	}

	public void setTiempoMotor3(Double tiempoMotor3) {
		this.tiempoMotor3 = tiempoMotor3;
	}

	public HashMap<String, Double> getTramoData() {
		return tramoData;
	}

	public void setTramoData(HashMap<String, Double> tramoData) {
		this.tramoData = tramoData;
	}
	
	@Override
	public String toString() {
		return "JoinedEvent [idEstado=" + idEstado + ", idConductor=" + idConductor + ", idVehiculo=" + idVehiculo
				+ ", fecha=" + fecha + ", distancia=" + distancia + ", cruiseActive=" + cruiseActive + ", rpmExcesivas="
				+ rpmExcesivas + ", frenadasBruscas=" + frenadasBruscas + ", aceleracionesBruscas="
				+ aceleracionesBruscas + ", cNoPredictiva2=" + cNoPredictiva2 + ", zRoja2=" + zRoja2 + ", zMasVerde2="
				+ zMasVerde2 + ", frenadasBruscas2=" + frenadasBruscas2 + ", aceleracionesBruscas2="
				+ aceleracionesBruscas2 + ", ralInec2=" + ralInec2 + ", tiempoConduccionCrucero2="
				+ tiempoConduccionCrucero2 + ", metrosAscendidos2=" + metrosAscendidos2 + ", metrosDescendidos2="
				+ metrosDescendidos2 + ", odometro2=" + odometro2 + ", totalFuel2=" + totalFuel2 + ", tiempoRal2="
				+ tiempoRal2 + ", consumoRal2=" + consumoRal2 + ", tiempoConduccion2=" + tiempoConduccion2
				+ ", nFreno3=" + nFreno3 + ", nEmbrague3=" + nEmbrague3 + ", tiempoMotor3=" + tiempoMotor3 + "]";
	}
	
	public static boolean isNull(String input) {
		
		if(input.equalsIgnoreCase("NULL") || input.equalsIgnoreCase("-999") || input.isBlank() || input.isEmpty()) {
			return true;
		}
		else {
			return false;
		}
	}
}