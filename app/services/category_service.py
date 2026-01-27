from typing import List, Optional
from sqlmodel import Session, select
from fastapi import HTTPException, status

from app.data_access.models import DimCategory
from app.domain.category import CategoryDomain

class CategoryService:
    """
    Service layer for managing Category business logic.
    
    This service orchestrates the transformation between CategoryDomain entities 
    and DimCategory data access models, ensuring business rules like name 
    uniqueness are enforced before persisting to the Silver/Gold layers.
    """

    def __init__(self, session: Session):
        """
        Initializes the CategoryService with a database session.

        Args:
            session (Session): The SQLModel/SQLAlchemy session for database operations.
        """
        self.session = session

    def _get_dim_category_or_404(self, category_id: int) -> DimCategory:
        """
        Internal helper to retrieve a category dimension or raise a 404 error.

        Args:
            category_id (int): The primary key ID of the category.

        Returns:
            DimCategory: The retrieved database model instance.

        Raises:
            HTTPException: 404 status if the category does not exist in the database.
        """
        category = self.session.get(DimCategory, category_id)
        if not category:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Category with ID {category_id} not found."
            )
        return category

    def create_category(self, category_in: CategoryDomain) -> DimCategory:
        """
        Validates business rules and persists a new Category to the database.

        Ensures that category names are unique before mapping the Domain 
        entity to the Data Access model.

        Args:
            category_in (CategoryDomain): The Pydantic domain model containing input data.

        Returns:
            DimCategory: The newly created database record.

        Raises:
            HTTPException: 400 status if a category with the same name already exists.
        """
        # 1. Business Rule: Category names should be unique
        statement = select(DimCategory).where(DimCategory.name == category_in.name)
        existing = self.session.exec(statement).first()
        
        if existing:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Category '{category_in.name}' already exists."
            )

        # 2. Map Domain -> Data Access
        new_category = DimCategory(
            name=category_in.name,
            description=category_in.description
        )

        self.session.add(new_category)
        self.session.commit()
        self.session.refresh(new_category)
        return new_category

    def get_all_categories(self) -> List[DimCategory]:
        """
        Retrieves all categories currently stored in the Gold Layer.

        Returns:
            List[DimCategory]: A list of all category database records.
        """
        return self.session.exec(select(DimCategory)).all()

    def get_category_by_id(self, category_id: int) -> DimCategory:
        """
        Retrieves a specific category by its unique identifier.

        Args:
            category_id (int): The ID of the category to retrieve.

        Returns:
            DimCategory: The retrieved category database record.

        Raises:
            HTTPException: 404 status via the internal helper if not found.
        """
        return self._get_dim_category_or_404(category_id)

    def update_category(self, category_id: int, category_in: CategoryDomain) -> DimCategory:
        """
        Updates an existing category's details in the database.

        Args:
            category_id (int): The ID of the category to be updated.
            category_in (CategoryDomain): The updated domain data.

        Returns:
            DimCategory: The updated database record.

        Raises:
            HTTPException: 404 status if the target category does not exist.
        """
        db_category = self._get_dim_category_or_404(category_id)

        # Update fields
        db_category.name = category_in.name
        db_category.description = category_in.description

        self.session.add(db_category)
        self.session.commit()
        self.session.refresh(db_category)
        return db_category

    def delete_category(self, category_id: int) -> dict:
        """
        Deletes a category from the dimension table.

        Note:
            In a production Star Schema, this should check for referential 
            integrity (ensuring no Products are linked to this category).

        Args:
            category_id (int): The ID of the category to delete.

        Returns:
            dict: A confirmation message indicating successful deletion.

        Raises:
            HTTPException: 404 status if the category does not exist.
        """
        db_category = self._get_dim_category_or_404(category_id)
        
        self.session.delete(db_category)
        self.session.commit()
        
        return {"detail": f"Category {category_id} deleted successfully."}

    def get_category_summary(self) -> List[dict]:
        """
        Queries the Gold Layer to provide a refined, analytical summary of categories.

        This fulfills the 'Querying Gold Layer' requirement by transforming 
        raw dimension data into a high-level report format.

        Returns:
            List[dict]: A list of refined dictionaries containing formatted category info.
        """
        categories = self.get_all_categories()
        return [
            {
                "category_id": c.id, 
                "label": c.name.upper(), 
                "info": c.description
            } 
            for c in categories
        ]