
public class StateObject {

	int counter;
	Double[] totgain = new Double[14] ;
	Double[] totloss = new Double[14] ;
	double avggain;
	double avgloss;
	double rs;
	double rsi;
	 
	StateObject()
	{
	
	}
	
	StateObject(int counter,
	Double[] totgain,
	Double[] totloss,
	double avggain,
	double avgloss,
	double rs,
	double rsi) {
		this.counter =counter;
		this.totgain = totgain;
		this.totloss = totloss;
		this.avggain = avggain;
		this.avgloss = avgloss;
		this.rs = rs;
		this.rsi = rsi;
		
		
		
	}
	
}
