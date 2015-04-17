package daogenerator.services.interfaces;

import java.util.List;

public interface Callback<T> {
	public void process(List<T> objList);
}
