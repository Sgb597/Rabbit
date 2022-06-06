package Pipline.Rabbit;

import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

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
				try {
					// Cast String distancia to Double
					String cleanInput = col.replaceAll("\"", "");
					Double distancia = Double.parseDouble(cleanInput);
					this.distancia = distancia;
				} catch(ClassCastException e) {
					e.printStackTrace();
				}
				break;
                case 5:
                	if(!isNull(col.replaceAll("\"", ""))) {
                		try {
                			String cleanInput = col.replaceAll("\"", "");
        					Double cruiseActive = Double.parseDouble(cleanInput);
                            this.cruiseActive = cruiseActive;
                        } catch(Exception e) {
                            e.printStackTrace();
                        }
                	} else {
                    	this.cruiseActive = 0.0;
                    }
                    break;
                case 6:
                	if(!isNull(col.replaceAll("\"", ""))) {
                		try {
                			String cleanInput = col.replaceAll("\"", "");
        					Double rpmExcesivas = Double.parseDouble(cleanInput);
                            this.rpmExcesivas = rpmExcesivas;
                        } catch(Exception e) {
                            e.printStackTrace();
                        }
                	} else {
                    	this.rpmExcesivas = 0.0;
                    }
                    break;
                case 7:
                	if(!isNull(col.replaceAll("\"", ""))) {
                		try {
                			String cleanInput = col.replaceAll("\"", "");
        					Double frenadasBruscas = Double.parseDouble(cleanInput);
                            this.frenadasBruscas = frenadasBruscas;
                        } catch(Exception e) {
                            e.printStackTrace();
                        }
                	} else {
                    	this.frenadasBruscas = 0.0;
                    }
                    break;
                case 8:
                	if(!isNull(col.replaceAll("\"", ""))) {
                		try {
                			String cleanInput = col.replaceAll("\"", "");
        					Double aceleracionesBruscas = Double.parseDouble(cleanInput);
                            this.aceleracionesBruscas = aceleracionesBruscas;
                        } catch(Exception e) {
                            e.printStackTrace();
                        }
                	} else {
                    	this.aceleracionesBruscas = 0.0;
                    }
                    break;
                case 9:
                	if(!isNull(col.replaceAll("\"", ""))) {
                		try {
                			String cleanInput = col.replaceAll("\"", "");
        					Double cNoPredictiva2 = Double.parseDouble(cleanInput);
                            this.cNoPredictiva2 = cNoPredictiva2;
                        } catch(Exception e) {
                            e.printStackTrace();
                        }
                	} else {
                    	this.cNoPredictiva2 = 0.0;
                    }
                    break;
                case 10:
                	if(!isNull(col.replaceAll("\"", ""))) {
                		try {
                			String cleanInput = col.replaceAll("\"", "");
        					Double zRoja2 = Double.parseDouble(cleanInput);
                            this.zRoja2 = zRoja2;
                        } catch(Exception e) {
                            e.printStackTrace();
                        }
                	} else {
                    	this.zRoja2 = 0.0;
                    }
                    break;
                case 11:
                	if(!isNull(col.replaceAll("\"", ""))) {
                		try {
                			String cleanInput = col.replaceAll("\"", "");
        					Double zMasVerde2 = Double.parseDouble(cleanInput);
                            this.zMasVerde2 = zMasVerde2;
                        } catch(Exception e) {
                            e.printStackTrace();
                        }
                	} else {
                    	this.zMasVerde2 = 0.0;
                    }
                    break;
                case 12:
                	if(!isNull(col.replaceAll("\"", ""))) {
                		try {
                			String cleanInput = col.replaceAll("\"", "");
        					Double frenadasBruscas2 = Double.parseDouble(cleanInput);
                            this.frenadasBruscas2 = frenadasBruscas2;
                        } catch(Exception e) {
                            e.printStackTrace();
                        }
                	} else {
                    	this.frenadasBruscas2 = 0.0;
                    }
                    break;
                case 13:
                	if(!isNull(col.replaceAll("\"", ""))) {
                		try {
                			String cleanInput = col.replaceAll("\"", "");
        					Double aceleracionesBruscas2 = Double.parseDouble(cleanInput);
                            this.aceleracionesBruscas2 = aceleracionesBruscas2;
                        } catch(Exception e) {
                            e.printStackTrace();
                        }
                	} else {
                    	this.aceleracionesBruscas2 = 0.0;
                    }
                    break;
                case 14:
                   	if(!isNull(col.replaceAll("\"", ""))) {
                		try {
                			String cleanInput = col.replaceAll("\"", "");
        					Double ralInnec2 = Double.parseDouble(cleanInput);
                            this.ralInec2 = ralInnec2;
                        } catch(Exception e) {
                            e.printStackTrace();
                        }
                	} else {
                    	this.ralInec2 = 0.0;
                    }
                    break;
                case 15:
                   	if(!isNull(col.replaceAll("\"", ""))) {
                		try {
                			String cleanInput = col.replaceAll("\"", "");
        					Double tiempoConduccionCrucero2 = Double.parseDouble(cleanInput);
                            this.tiempoConduccionCrucero2 = tiempoConduccionCrucero2;
                        } catch(Exception e) {
                            e.printStackTrace();
                        }
                	} else {
                    	this.tiempoConduccionCrucero2 = 0.0;
                    }
                    break;
                case 16:
                   	if(!isNull(col.replaceAll("\"", ""))) {
                		try {
                			String cleanInput = col.replaceAll("\"", "");
        					Double metrosAscendidos2 = Double.parseDouble(cleanInput);
                            this.metrosAscendidos2 = metrosAscendidos2;
                        } catch(Exception e) {
                            e.printStackTrace();
                        }
                	} else {
                    	this.metrosAscendidos2 = 0.0;
                    }
                    break;
                case 17:
                   	if(!isNull(col.replaceAll("\"", ""))) {
                		try {
                			String cleanInput = col.replaceAll("\"", "");
        					Double metrosDescendidos2 = Double.parseDouble(cleanInput);
                            this.metrosDescendidos2 = metrosDescendidos2;
                        } catch(Exception e) {
                            e.printStackTrace();
                        }
                	} else {
                    	this.metrosDescendidos2 = 0.0;
                    }
                    break;
                case 18:
                   	if(!isNull(col.replaceAll("\"", ""))) {
                		try {
                			String cleanInput = col.replaceAll("\"", "");
        					Double odometro2 = Double.parseDouble(cleanInput);
                            this.odometro2 = odometro2;
                        } catch(Exception e) {
                            e.printStackTrace();
                        }
                	} else {
                    	this.odometro2 = 0.0;
                    }
                    break;
                case 19:
                   	if(!isNull(col.replaceAll("\"", ""))) {
                		try {
                			String cleanInput = col.replaceAll("\"", "");
        					Double totalFuel2 = Double.parseDouble(cleanInput);
                            this.totalFuel2 = totalFuel2;
                        } catch(Exception e) {
                            e.printStackTrace();
                        }
                	} else {
                    	this.totalFuel2 = 0.0;
                    }
                    break;
                case 20:
                   	if(!isNull(col.replaceAll("\"", ""))) {
                		try {
                			String cleanInput = col.replaceAll("\"", "");
        					Double tiempoRal2 = Double.parseDouble(cleanInput);
                            this.tiempoRal2 = tiempoRal2;
                        } catch(Exception e) {
                            e.printStackTrace();
                        }
                	} else {
                    	this.tiempoRal2 = 0.0;
                    }
                    break;
                case 21:
                   	if(!isNull(col.replaceAll("\"", ""))) {
                		try {
                			String cleanInput = col.replaceAll("\"", "");
        					Double consumoRal2 = Double.parseDouble(cleanInput);
                            this.consumoRal2 = consumoRal2;
                        } catch(Exception e) {
                            e.printStackTrace();
                        }
                	} else {
                    	this.consumoRal2 = 0.0;
                    }
                    break;
                case 22:
                   	if(!isNull(col.replaceAll("\"", ""))) {
                		try {
                			String cleanInput = col.replaceAll("\"", "");
        					Double tiempoConduccion2 = Double.parseDouble(cleanInput);
                            this.tiempoConduccion2 = tiempoConduccion2;
                        } catch(Exception e) {
                            e.printStackTrace();
                        }
                	} else {
                    	this.tiempoConduccion2 = 0.0;
                    }
                    break;
                case 23:
                   	if(!isNull(col.replaceAll("\"", ""))) {
                		try {
                			String cleanInput = col.replaceAll("\"", "");
        					Double nFreno3 = Double.parseDouble(cleanInput);
                            this.nFreno3 = nFreno3;
                        } catch(Exception e) {
                            e.printStackTrace();
                        }
                	} else {
                    	this.nFreno3 = 0.0;
                    }
                    break;
                case 24:
                   	if(!isNull(col.replaceAll("\"", ""))) {
                		try {
                			String cleanInput = col.replaceAll("\"", "");
        					Double nEmbrague3 = Double.parseDouble(cleanInput);
                            this.nEmbrague3 = nEmbrague3;
                        } catch(Exception e) {
                            e.printStackTrace();
                        }
                	} else {
                    	this.nEmbrague3 = 0.0;
                    }
                    break;
                case 25:
                   	if(!isNull(col.replaceAll("\"", ""))) {
                		try {
                			String cleanInput = col.replaceAll("\"", "");
        					Double tiempoMotor3 = Double.parseDouble(cleanInput);
                            this.tiempoMotor3 = tiempoMotor3;
                        } catch(Exception e) {
                            e.printStackTrace();
                        }
                	} else {
                    	this.tiempoMotor3 = 0.0;
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