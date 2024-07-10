package model.dao.impl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import db.DB;
import db.DbException;
import model.dao.SellerDAO;
import model.entities.Department;
import model.entities.Seller;

public class SellerDaoJDBC implements SellerDAO {
	
	private Connection conn;
	
	public SellerDaoJDBC(Connection conn) {
		this.conn = conn;
	}

	@Override
	public void insert(Seller seller) {
		 PreparedStatement statement = null;
		 
		 try {
			 statement = this.conn.prepareStatement(
				 "INSERT INTO seller "
				+"(Name, Email, BirthDate, BaseSalary, DepartmentId) "
				+ "VALUES (?, ?, ?, ?, ?)",
				Statement.RETURN_GENERATED_KEYS);
			 
			 statement.setString(1, seller.getName());
			 statement.setString(2, seller.getEmail());
			 statement.setDate(3, new java.sql.Date(seller.getBirthDate().getTime()));
			 statement.setDouble(4, seller.getBaseSalary());
			 statement.setInt(5, seller.getDepartment().getId());
			 
			 int rowsAffect = statement.executeUpdate();
			 
			 if (rowsAffect > 0) {
				 ResultSet resultSet = statement.getGeneratedKeys();
				 if (resultSet.next()) {
					 int id = resultSet.getInt(1);
					 seller.setId(id);
				 }
				 
				 DB.closeResultSet(resultSet);
			 } else {
				 throw new DbException("Unexpected error: no rows affected!");
			 }
			 
			 
		 } catch( SQLException e) {
			 throw new DbException(e.getMessage());
		 } finally {
			DB.closeStatement(statement); 
		 }
		 
		
	}

	@Override
	public void update(Seller seller) {
		PreparedStatement statement = null;
		 
		 try {
			 statement = this.conn.prepareStatement(
				 "UPDATE seller "
				+"SET Name = ?, Email = ?, BirthDate = ?, BaseSalary = ?, DepartmentId = ? "
				+"WHERE id = ?"
				);
			 
			 statement.setString(1, seller.getName());
			 statement.setString(2, seller.getEmail());
			 statement.setDate(3, new java.sql.Date(seller.getBirthDate().getTime()));
			 statement.setDouble(4, seller.getBaseSalary());
			 statement.setInt(5, seller.getDepartment().getId());
			 statement.setInt(6, seller.getId());
			 
			 statement.executeUpdate();
			 
		 } catch( SQLException e) {
			 throw new DbException(e.getMessage());
		 } finally {
			DB.closeStatement(statement); 
		 }
		
	}

	@Override
	public void delete(Integer id) {
		PreparedStatement statement = null;
		
		try {
			statement = conn.prepareStatement(
					 "DELETE from seller " 
					+"WHERE id = ?");
			
			statement.setInt(1, id);
			statement.executeUpdate();
			
		} catch(SQLException e) {
			throw new DbException(e.getMessage());
		} finally {
			DB.closeStatement(statement);
		}
	}

	@Override
	public Seller findById(Integer id) {
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		try {
			statement = conn.prepareStatement(
					"SELECT seller.*,department.Name as DepName "
					+ "FROM seller INNER JOIN department "
					+ "ON seller.DepartmentId = department.Id "
					+ "WHERE seller.Id = ?");
			statement.setInt(1, id);
			
			
			resultSet = statement.executeQuery();
			
			if (resultSet.next()) {
				// need to associate both objects Department and Seller
				Department department = instanciateDepartment(resultSet);
				
				Seller seller = instanciateSeller(resultSet, department);
				return seller;
			}
			
			return null;
			
		} catch (SQLException e) {
			throw new DbException(e.getMessage());
		} finally {
			DB.closeStatement(statement);
			DB.closeResultSet(resultSet);
		}
	}

	private Seller instanciateSeller(ResultSet resultSet, Department department) throws SQLException {
		Seller seller = new Seller();
		seller.setId(resultSet.getInt("Id"));
		seller.setName(resultSet.getString("Name"));
		seller.setEmail(resultSet.getString("Email"));
		seller.setBaseSalary(resultSet.getDouble("BaseSalary"));
		seller.setBirthDate(resultSet.getDate("BirthDate"));
		seller.setDepartment(department);
		return seller;
	}

	private Department instanciateDepartment(ResultSet resultSet) throws SQLException {
		Department department = new Department();
		department.setId(resultSet.getInt("Id"));
		department.setName(resultSet.getString("name"));
		return department;
	}

	@Override
	public List<Seller> findAll() {
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		
		try {
			statement = this.conn.prepareStatement(
					 "SELECT seller.*, department.Name as DepName "
					+ "FROM seller INNER JOIN department "
					+ "ON seller.DepartmentId = department.Id "
					+ "ORDER BY NAME");
			
			resultSet = statement.executeQuery();
			
			List<Seller> sellers = new ArrayList<>();
			Map<Integer, Department> depMap = new HashMap<>();
			
			while(resultSet.next()) {
				Department dep = depMap.get(resultSet.getInt("DepartmentId"));
				
				if (dep == null) {
					dep = this.instanciateDepartment(resultSet);
					depMap.put(resultSet.getInt("DepartmentId"), dep);
				}
				
				Seller seller = this.instanciateSeller(resultSet, dep);
				sellers.add(seller);
			}
			
			return sellers;
			
		} catch( SQLException e) {
			throw new DbException(e.getMessage());
		} finally {
			DB.closeStatement(statement);
			DB.closeResultSet(resultSet);
		}
	}

	@Override
	public List<Seller> findByDepartment(Department department) {
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		
		try {
			statement = this.conn.prepareStatement(
					 "SELECT seller.*, department.Name as DepName "
					+ "FROM seller INNER JOIN department "
					+ "ON seller.DepartmentId = department.Id "
					+ "WHERE DepartmentId = ? "
					+ "ORDER BY NAME");
			
			statement.setInt(1, department.getId());
			resultSet = statement.executeQuery();
			
			List<Seller> sellers = new ArrayList<>();
			Map<Integer, Department> depMap = new HashMap<>();
			
			while(resultSet.next()) {
				Department dep = depMap.get(resultSet.getInt("DepartmentId"));
				
				if (dep == null) {
					dep = this.instanciateDepartment(resultSet);
					depMap.put(resultSet.getInt("DepartmentId"), dep);
				}
				
				Seller seller = this.instanciateSeller(resultSet, dep);
				sellers.add(seller);
			}
			
			return sellers;
			
		} catch( SQLException e) {
			throw new DbException(e.getMessage());
		} finally {
			DB.closeStatement(statement);
			DB.closeResultSet(resultSet);
		}
	}
}
