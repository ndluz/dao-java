package model.dao;

import java.util.List;

import model.entities.Department;

public interface DepartmentDAO {
	void insert(Department department);
	void update(Department department);
	void delete(Integer id);
	Department findById(Integer id);
	List<Department> findAll();
}
