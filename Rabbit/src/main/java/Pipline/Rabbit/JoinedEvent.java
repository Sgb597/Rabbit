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
	private HashMap<String, Double> tramoData;

	public JoinedEvent() {}
	
	public JoinedEvent(String value) {
		this.tramoData = new HashMap<String, Double>();
		
		int i = 0;
		for(String col: value.split(",")){
            switch(i){
            case 0:
                try {
                    String idVehiculo = col;
                    this.idVehiculo = idVehiculo;
                } catch(Exception e) {
                    e.printStackTrace();
                }
                break;
            case 1:
                try {
                    String idConductor = col;
                    this.idConductor = idConductor;
                } catch(Exception e) {
                    e.printStackTrace();
                }
                break;
            case 2:
                try {
                    String idEstado = col;
                    this.idEstado = idEstado;
                } catch(Exception e) {
                    e.printStackTrace();
                }
                break;
            case 3: 
                try {
                    SimpleDateFormat dateParser = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
                    String dateAsString = col;
                    Date date = dateParser.parse(dateAsString);
                    Timestamp timestamp = new java.sql.Timestamp(date.getTime());
                    this.fecha = timestamp;
                } catch(ParseException e) {
                    e.printStackTrace();
                }
                break;
            case 4:
            	if(!isNull(col)) {
            		try {
    					// Cast String distancia to Double
    					String cleanInput = col;
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
                	if(!isNull(col)) {
                		try {
                			String cleanInput = col;
        					Double cruiseActive = Double.parseDouble(cleanInput);
                            this.tramoData.put("cruiseActive", cruiseActive);
                        } catch(Exception e) {
                            e.printStackTrace();
                        }
                	} else {
                    	this.tramoData.put("cruiseActive", 0.0);
                    }
                    break;
                case 6:
                	if(!isNull(col)) {
                		try {
                			String cleanInput = col;
        					Double rpmExcesivas = Double.parseDouble(cleanInput);
                            this.tramoData.put("rpmExcesivas", rpmExcesivas);
                        } catch(Exception e) {
                            e.printStackTrace();
                        }
                	} else {
                    	this.tramoData.put("rpmExcesivas", 0.0);

                    }
                    break;
                case 7:
                	if(!isNull(col)) {
                		try {
                			String cleanInput = col;
        					Double frenadasBruscas = Double.parseDouble(cleanInput);
                            this.tramoData.put("frenadasBruscas", frenadasBruscas);
                        } catch(Exception e) {
                            e.printStackTrace();
                        }
                	} else {
                    	this.tramoData.put("frenadasBruscas", 0.0);
                    }
                    break;
                case 8:
                	if(!isNull(col)) {
                		try {
                			String cleanInput = col;
        					Double aceleracionesBruscas = Double.parseDouble(cleanInput);
                            this.tramoData.put("aceleracionesBruscas", aceleracionesBruscas);
                        } catch(Exception e) {
                            e.printStackTrace();
                        }
                	} else {
                    	this.tramoData.put("aceleracionesBruscas", 0.0);

                    }
                    break;
                case 9:
                	if(!isNull(col)) {
                		try {
                			String cleanInput = col;
        					Double cNoPredictiva2 = Double.parseDouble(cleanInput);
                            this.tramoData.put("cNoPredictiva2", cNoPredictiva2);
                        } catch(Exception e) {
                            e.printStackTrace();
                        }
                	} else {
                        this.tramoData.put("cNoPredictiva2", 0.0);
                    }
                    break;
                case 10:
                	if(!isNull(col)) {
                		try {
                			String cleanInput = col;
        					Double zRoja2 = Double.parseDouble(cleanInput);
                            this.tramoData.put("zRoja2", zRoja2);
                        } catch(Exception e) {
                            e.printStackTrace();
                        }
                	} else {
                        this.tramoData.put("zRoja2", 0.0);
                    }
                    break;
                case 11:
                	if(!isNull(col)) {
                		try {
                			String cleanInput = col;
        					Double zMasVerde2 = Double.parseDouble(cleanInput);
                            this.tramoData.put("zMasVerde2", zMasVerde2);
                        } catch(Exception e) {
                            e.printStackTrace();
                        }
                	} else {
                        this.tramoData.put("zMasVerde2", 0.0);
                    }
                    break;
                case 12:
                	if(!isNull(col)) {
                		try {
                			String cleanInput = col;
        					Double frenadasBruscas2 = Double.parseDouble(cleanInput);
                            this.tramoData.put("frenadasBruscas2", frenadasBruscas2);
                        } catch(Exception e) {
                            e.printStackTrace();
                        }
                	} else {
                        this.tramoData.put("frenadasBruscas2", 0.0);
                    }
                    break;
                case 13:
                	if(!isNull(col)) {
                		try {
                			String cleanInput = col;
        					Double aceleracionesBruscas2 = Double.parseDouble(cleanInput);
                            this.tramoData.put("aceleracionesBruscas2", aceleracionesBruscas2);
                        } catch(Exception e) {
                            e.printStackTrace();
                        }
                	} else {
                        this.tramoData.put("aceleracionesBruscas2", 0.0);
                    }
                    break;
                case 14:
                   	if(!isNull(col)) {
                		try {
                			String cleanInput = col;
        					Double ralInnec2 = Double.parseDouble(cleanInput);
                            this.tramoData.put("ralInnec2", ralInnec2);
                        } catch(Exception e) {
                            e.printStackTrace();
                        }
                	} else {
                        this.tramoData.put("ralInnec2", 0.0);
                    }
                    break;
                case 15:
                   	if(!isNull(col)) {
                		try {
                			String cleanInput = col;
        					Double tiempoConduccionCrucero2 = Double.parseDouble(cleanInput);
                            this.tramoData.put("tiempoConduccionCrucero2", tiempoConduccionCrucero2);
                        } catch(Exception e) {
                            e.printStackTrace();
                        }
                	} else {
                        this.tramoData.put("tiempoConduccionCrucero2", 0.0);
                    }
                    break;
                case 16:
                   	if(!isNull(col)) {
                		try {
                			String cleanInput = col;
        					Double metrosAscendidos2 = Double.parseDouble(cleanInput);
                            this.tramoData.put("metrosAscendidos2", metrosAscendidos2);
                        } catch(Exception e) {
                            e.printStackTrace();
                        }
                	} else {
                        this.tramoData.put("metrosAscendidos2", 0.0);
                    }
                    break;
                case 17:
                   	if(!isNull(col)) {
                		try {
                			String cleanInput = col;
        					Double metrosDescendidos2 = Double.parseDouble(cleanInput);
                            this.tramoData.put("metrosDescendidos2", metrosDescendidos2);
                        } catch(Exception e) {
                            e.printStackTrace();
                        }
                	} else {
                        this.tramoData.put("metrosDescendidos2", 0.0);
                    }
                    break;
                case 18:
                   	if(!isNull(col)) {
                		try {
                			String cleanInput = col;
        					Double odometro2 = Double.parseDouble(cleanInput);
                            this.tramoData.put("odometro2", odometro2);
                        } catch(Exception e) {
                            e.printStackTrace();
                        }
                	} else {
                        this.tramoData.put("odometro2", 0.0);
                    }
                    break;
                case 19:
                   	if(!isNull(col)) {
                		try {
                			String cleanInput = col;
        					Double totalFuel2 = Double.parseDouble(cleanInput);
                            this.tramoData.put("totalFuel2", totalFuel2);
                        } catch(Exception e) {
                            e.printStackTrace();
                        }
                	} else {
                        this.tramoData.put("totalFuel2", 0.0);
                    }
                    break;
                case 20:
                   	if(!isNull(col)) {
                		try {
                			String cleanInput = col;
        					Double tiempoRal2 = Double.parseDouble(cleanInput);
                            this.tramoData.put("tiempoRal2", tiempoRal2);
                        } catch(Exception e) {
                            e.printStackTrace();
                        }
                	} else {
                        this.tramoData.put("tiempoRal2", 0.0);
                    }
                    break;
                case 21:
                   	if(!isNull(col)) {
                		try {
                			String cleanInput = col;
        					Double consumoRal2 = Double.parseDouble(cleanInput);
                            this.tramoData.put("consumoRal2", consumoRal2);
                        } catch(Exception e) {
                            e.printStackTrace();
                        }
                	} else {
                        this.tramoData.put("consumoRal2", 0.0);
                    }
                    break;
                case 22:
                   	if(!isNull(col)) {
                		try {
                			String cleanInput = col;
        					Double tiempoConduccion2 = Double.parseDouble(cleanInput);
                            this.tramoData.put("tiempoConduccion2", tiempoConduccion2);
                        } catch(Exception e) {
                            e.printStackTrace();
                        }
                	} else {
                        this.tramoData.put("tiempoConduccion2", 0.0);
                    }
                    break;
                case 23:
                   	if(!isNull(col)) {
                		try {
                			String cleanInput = col;
        					Double nFreno3 = Double.parseDouble(cleanInput);
                            this.tramoData.put("nFreno3", nFreno3);
                        } catch(Exception e) {
                            e.printStackTrace();
                        }
                	} else {
                        this.tramoData.put("nFreno3", 0.0);
                    }
                    break;
                case 24:
                   	if(!isNull(col)) {
                		try {
                			String cleanInput = col;
        					Double nEmbrague3 = Double.parseDouble(cleanInput);
                            this.tramoData.put("nEmbrague3", nEmbrague3);
                        } catch(Exception e) {
                            e.printStackTrace();
                        }
                	} else {
                        this.tramoData.put("nEmbrague3", 0.0);
                    }
                    break;
                case 25:
                   	if(!isNull(col)) {
                		try {
                			String cleanInput = col;
        					Double tiempoMotor3 = Double.parseDouble(cleanInput);
                            this.tramoData.put("tiempoMotor3", tiempoMotor3);
                        } catch(Exception e) {
                            e.printStackTrace();
                        }
                	} else {
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

	public HashMap<String, Double> getTramoData() {
		return tramoData;
	}

	public void setTramoData(HashMap<String, Double> tramoData) {
		this.tramoData = tramoData;
	}

	@Override
	public String toString() {
		return "JoinedEvent [idEstado=" + idEstado + ", idConductor=" + idConductor + ", idVehiculo=" + idVehiculo
				+ ", fecha=" + fecha + ", distancia=" + distancia + "]";
	}

	public static boolean isNull(String input) {
		if(input.equalsIgnoreCase("NULL") || input.equalsIgnoreCase("-999")) {
			return true;
		}
		else {
			return false;
		}
	}
}