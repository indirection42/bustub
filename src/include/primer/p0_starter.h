//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// p0_starter.h
//
// Identification: src/include/primer/p0_starter.h
//
// Copyright (c) 2015-2020, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <stdexcept>
#include <vector>

#include "common/exception.h"

namespace bustub {

/**
 * The Matrix type defines a common
 * interface for matrix operations.
 */
template <typename T>
class Matrix {
 protected:
  /**
   * TODO(P0): Add implementation
   *
   * Construct a new Matrix instance.
   * @param rows The number of rows
   * @param cols The number of columns
   *
   */
  Matrix(int rows, int cols) : rows_{rows}, cols_{cols}, linear_{new T[rows_ * cols_]} {}

  /** The number of rows in the matrix */
  int rows_;
  /** The number of columns in the matrix */
  int cols_;

  /**
   * TODO(P0): Allocate the array in the constructor.
   * TODO(P0): Deallocate the array in the destructor.
   * A flattened array containing the elements of the matrix.
   */
  T *linear_ = nullptr;

 public:
  /** @return The number of rows in the matrix */
  virtual auto GetRowCount() const -> int = 0;

  /** @return The number of columns in the matrix */
  virtual auto GetColumnCount() const -> int = 0;

  /**
   * Get the (i,j)th matrix element.
   *
   * Throw OUT_OF_RANGE if either index is out of range.
   *
   * @param i The row index
   * @param j The column index
   * @return The (i,j)th matrix element
   * @throws OUT_OF_RANGE if either index is out of range
   */
  virtual auto GetElement(int i, int j) const -> T = 0;

  /**
   * Set the (i,j)th matrix element.
   *
   * Throw OUT_OF_RANGE if either index is out of range.
   *
   * @param i The row index
   * @param j The column index
   * @param val The value to insert
   * @throws OUT_OF_RANGE if either index is out of range
   */
  virtual void SetElement(int i, int j, T val) = 0;

  /**
   * Fill the elements of the matrix from `source`.
   *
   * Throw OUT_OF_RANGE in the event that `source`
   * does not contain the required number of elements.
   *
   * @param source The source container
   * @throws OUT_OF_RANGE if `source` is incorrect size
   */
  virtual void FillFrom(const std::vector<T> &source) = 0;

  /**
   * Destroy a matrix instance.
   * TODO(P0): Add implementation
   */
  virtual ~Matrix() { delete[] linear_; }
};

/**
 * The RowMatrix type is a concrete matrix implementation.
 * It implements the interface defined by the Matrix type.
 */
template <typename T>
class RowMatrix : public Matrix<T> {
 public:
  /**
   * TODO(P0): Add implementation
   *
   * Construct a new RowMatrix instance.
   * @param rows The number of rows
   * @param cols The number of columns
   */
  RowMatrix(int rows, int cols) : Matrix<T>(rows, cols), data_{new T *[rows]} {
    for (int i = 0; i < rows; i++) {
      data_[i] = this->linear_ + i * this->cols_;
    }
  }

  /**
   * TODO(P0): Add implementation
   * @return The number of rows in the matrix
   */
  int GetRowCount() const override { return this->rows_; }

  /**
   * TODO(P0): Add implementation
   * @return The number of columns in the matrix
   */
  int GetColumnCount() const override { return this->cols_; }

  /**
   * TODO(P0): Add implementation
   *
   * Get the (i,j)th matrix element.
   *
   * Throw OUT_OF_RANGE if either index is out of range.
   *
   * @param i The row index
   * @param j The column index
   * @return The (i,j)th matrix element
   * @throws OUT_OF_RANGE if either index is out of range
   */
  T GetElement(int i, int j) const override {
    if (i < 0 || i >= this->rows_ || j < 0 || j >= this->cols_) {
      throw Exception(ExceptionType::OUT_OF_RANGE, "index is out of range");
    }
    return data_[i][j];
  }

  /**
   * Set the (i,j)th matrix element.
   *
   * Throw OUT_OF_RANGE if either index is out of range.
   *
   * @param i The row index
   * @param j The column index
   * @param val The value to insert
   * @throws OUT_OF_RANGE if either index is out of range
   */
  void SetElement(int i, int j, T val) override {
    if (i < 0 || i >= this->rows_ || j < 0 || j >= this->cols_) {
      throw Exception(ExceptionType::OUT_OF_RANGE, "index is out of range");
    }
    data_[i][j] = val;
  }

  /**
   * TODO(P0): Add implementation
   *
   * Fill the elements of the matrix from `source`.
   *
   * Throw OUT_OF_RANGE in the event that `source`
   * does not contain the required number of elements.
   *
   * @param source The source container
   * @throws OUT_OF_RANGE if `source` is incorrect size
   */
  void FillFrom(const std::vector<T> &source) override {
    if (source.size() != static_cast<unsigned int>(this->rows_ * this->cols_)) {
      throw Exception(ExceptionType::OUT_OF_RANGE, "`source` is incorrect size");
    }
    T *p = this->linear_;
    for (const T &x : source) {
      *p = x;
      p++;
    }
  }

  /**
   * TODO(P0): Add implementation
   *
   * Destroy a RowMatrix instance.
   */
  ~RowMatrix() override { delete[] data_; };

 private:
  /**
   * A 2D array containing the elements of the matrix in row-major format.
   *
   * TODO(P0):
   * - Allocate the array of row pointers in the constructor.
   * - Use these pointers to point to corresponding elements of the `linear` array.
   * - Don't forget to deallocate the array in the destructor.
   */
  T **data_;
};

/**
 * The RowMatrixOperations class defines operations
 * that may be performed on instances of `RowMatrix`.
 */
template <typename T>
class RowMatrixOperations {
 public:
  /**
   * Compute (`matrixA` + `matrixB`) and return the result.
   * Return `nullptr` if dimensions mismatch for input matrices.
   * @param matrixA Input matrix
   * @param matrixB Input matrix
   * @return The result of matrix addition
   */
  static std::unique_ptr<RowMatrix<T>> Add(std::unique_ptr<RowMatrix<T>> matrixA,
                                           std::unique_ptr<RowMatrix<T>> matrixB) {
    // TODO(P0): Add implementation
    if (matrixA->GetRowCount() == matrixB->GetRowCount() && matrixA->GetColumnCount() == matrixB->GetColumnCount()) {
      int rows = matrixA->GetRowCount();
      int cols = matrixA->GetColumnCount();
      std::unique_ptr<RowMatrix<T>> matrix_result{new RowMatrix<T>{rows, cols}};
      for (int i = 0; i < rows; i++) {
        for (int j = 0; j < cols; j++) {
          matrix_result->SetElement(i, j, matrixA->GetElement(i, j) + matrixB->GetElement(i, j));
        }
        return matrix_result;
      }
    }
    return std::unique_ptr<RowMatrix<T>>(nullptr);
  }

  /**
   * Compute the matrix multiplication (`matrixA` * `matrixB` and return the result.
   * Return `nullptr` if dimensions mismatch for input matrices.
   * @param matrixA Input matrix
   * @param matrixB Input matrix
   * @return The result of matrix multiplication
   */
  static std::unique_ptr<RowMatrix<T>> Multiply(const std::unique_ptr<RowMatrix<T>> *matrixA,
                                                const std::unique_ptr<RowMatrix<T>> matrixB) {
    // TODO(P0): Add implementation
    if (matrixA->GetColumnCount() == matrixB->GetRowCount()) {
      int rows_a = matrixA->GetRowCount();
      int cols_a = matrixA->GetColumnCount();
      int cols_b = matrixB->GetColumnCount();
      auto matrix_result = std::make_unique<RowMatrix<T>>{rows_a, cols_b};
      for (int i = 0; i < rows_a; i++) {
        for (int j = 0; j < cols_a; j++) {
          T temp = matrixA->GetElement(i, j);
          for (int k = 0; k < cols_b; k++) {
            matrix_result->SetElement(i, k, matrix_result->GetElement(i, k) + temp * matrixB->GetElement(j, k));
          }
        }
      }
      return matrix_result;
    }
    return std::unique_ptr<RowMatrix<T>>(nullptr);
  }

  /**
   * Simplified General Matrix Multiply operation. Compute (`matrixA` * `matrixB` + `matrixC`).
   * Return `nullptr` if dimensions mismatch for input matrices.
   * @param matrixA Input matrix
   * @param matrixB Input matrix
   * @param matrixC Input matrix
   * @return The result of general matrix multiply
   */
  static std::unique_ptr<RowMatrix<T>> GEMM(std::unique_ptr<RowMatrix<T>> matrixA,
                                            std::unique_ptr<RowMatrix<T>> matrixB,
                                            std::unique_ptr<RowMatrix<T>> matrixC) {
    // TODO(P0): Add implementation
    auto matrix_multiply_result = Multiply(matrixA, matrixB);
    if (matrix_multiply_result) {
      return Add(matrix_multiply_result, matrixC);
    }
    return std::unique_ptr<RowMatrix<T>>(nullptr);
  }
};
}  // namespace bustub
