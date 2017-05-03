package neu.finalProject;


public class MovieRelation {
   public int movieA;
   public int movieB;
   public int relation;
   
   public MovieRelation(int a, int b, int r){
	  movieA= a;
	  movieB = b;
	  relation = r;
   }

public int getMovieA() {
	return movieA;
}

public void setMovieA(int movieA) {
	this.movieA = movieA;
}

public int getMovieB() {
	return movieB;
}

public void setMovieB(int movieB) {
	this.movieB = movieB;
}

public int getRelation() {
	return relation;
}

public void setRelation(int relation) {
	this.relation = relation;
}
   
}
