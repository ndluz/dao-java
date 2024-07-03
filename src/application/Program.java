package application;

import java.util.Date;

import model.dao.DaoFactory;
import model.dao.SellerDAO;
import model.entities.Department;
import model.entities.Seller;

public class Program {

	public static void main(String[] args) {
		Department department = new Department(1, "Books");

		Seller seller = new Seller(321, "Anderson", "anderson@gmail.com", new Date(), 3000.0, department);
		
		SellerDAO sellerDao = DaoFactory.createSellerDao();
		System.out.println(seller);
	}

}
