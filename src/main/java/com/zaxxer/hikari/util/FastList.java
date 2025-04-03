/*
 * Copyright (C) 2013, 2014 Brett Wooldridge
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.zaxxer.hikari.util;

import java.io.Serializable;
import java.lang.reflect.Array;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.NoSuchElementException;
import java.util.RandomAccess;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;

/**
 * Fast list without range checking.
 *
 * @author Brett Wooldridge
 */
@SuppressWarnings("NullableProblems")
public final class FastList<T> implements List<T>, RandomAccess, Serializable
{
   /**
    FastList和ArrayList的add方法区别：
      ArrayList方法层级比FastList多，出入栈更频繁。
      由于ArrayList使用无参构造时，elementData数组变量是个空数组，需要在首次add时触发数组初始化，多了一些逻辑判断（对于Hikari来说，创建FastList和ArrayList时都传入了初始容量，这些逻辑判断都是无用的）。
      FastList相比ArrayList去除了modCount的自增操作。
      ArrayList扩容的计算逻辑相对复杂，考虑了很多边界条件。拷贝数组使用Arrays.copyOf方法，其底层也是调用System.arraycopy，但是调用栈很深。
    */


   private static final long serialVersionUID = -4598088075242913858L;

   // 容器里元素的Class
   private final Class<?> clazz;

   // 数组
   private T[] elementData;

   // 容器中元素个数
   private int size;

   /**
    * Construct a FastList with a default size of 32.
    * @param clazz the Class stored in the collection
    */
   @SuppressWarnings("unchecked")
   public FastList(Class<?> clazz)
   {
      this.elementData = (T[]) Array.newInstance(clazz, 32);
      this.clazz = clazz;
   }

   /**
    * Construct a FastList with a specified size.
    * @param clazz the Class stored in the collection
    * @param capacity the initial size of the FastList
    */
   @SuppressWarnings("unchecked")
   public FastList(Class<?> clazz, int capacity)
   {
      this.elementData = (T[]) Array.newInstance(clazz, capacity);
      this.clazz = clazz;
   }

   /**
    * Add an element to the tail of the FastList.
    *
    * @param element the element to add
    */
   @Override
   public boolean add(T element)
   {
      if (size < elementData.length) {
         // 如果元素个数没有超过数组长度，直接放入数组
         elementData[size++] = element;
      }
      else {
         // overflow-conscious code
         // 否则两倍扩容，拷贝数组
         final var oldCapacity = elementData.length;
         final var newCapacity = oldCapacity << 1;
         @SuppressWarnings("unchecked")
         final var newElementData = (T[]) Array.newInstance(clazz, newCapacity);
         System.arraycopy(elementData, 0, newElementData, 0, oldCapacity);
         newElementData[size++] = element;
         elementData = newElementData;
      }

      return true;
   }

   /**
    * Get the element at the specified index.
    *
    * @param index the index of the element to get
    * @return the element, or ArrayIndexOutOfBounds is thrown if the index is invalid
    */
   @Override
   public T get(int index)
   {
      return elementData[index];
   }

   /**
    * Remove the last element from the list.  No bound check is performed, so if this
    * method is called on an empty list and ArrayIndexOutOfBounds exception will be
    * thrown.
    *
    * @return the last element of the list
    */
   public T removeLast()
   {
      T element = elementData[--size];
      elementData[size] = null;
      return element;
   }

   /**
    * This remove method is most efficient when the element being removed
    * is the last element.  Equality is identity based, not equals() based.
    * Only the first matching element is removed.
    *
    * @param element the element to remove
    */
   @Override
   public boolean remove(Object element)
   {
      for (var index = size - 1; index >= 0; index--) {
         if (element == elementData[index]) {
            final var numMoved = size - index - 1;
            if (numMoved > 0) {
               System.arraycopy(elementData, index + 1, elementData, index, numMoved);
            }
            elementData[--size] = null;
            return true;
         }
      }

      return false;
   }

   /**
    * Clear the FastList.
    */
   @Override
   public void clear()
   {
      for (var i = 0; i < size; i++) {
         elementData[i] = null;
      }

      size = 0;
   }

   /**
    * Get the current number of elements in the FastList.
    *
    * @return the number of current elements
    */
   @Override
   public int size()
   {
      return size;
   }

   /** {@inheritDoc} */
   @Override
   public boolean isEmpty()
   {
      return size == 0;
   }

   /** {@inheritDoc} */
   @Override
   public T set(int index, T element)
   {
      T old = elementData[index];
      elementData[index] = element;
      return old;
   }

   /** {@inheritDoc} */
   @Override
   public T remove(int index)
   {
      if (size == 0) {
         return null;
      }

      final T old = elementData[index];

      final var numMoved = size - index - 1;
      if (numMoved > 0) {
         System.arraycopy(elementData, index + 1, elementData, index, numMoved);
      }

      elementData[--size] = null;

      return old;
   }

   /** {@inheritDoc} */
   @Override
   public boolean contains(Object o)
   {
      throw new UnsupportedOperationException();
   }

   /** {@inheritDoc} */
   @Override
   public Iterator<T> iterator()
   {
      return new Iterator<>() {
         private int index;

         @Override
         public boolean hasNext()
         {
            return index < size;
         }

         @Override
         public T next()
         {
            if (index < size) {
               return elementData[index++];
            }

            throw new NoSuchElementException("No more elements in FastList");
         }
      };
   }

   /** {@inheritDoc} */
   @Override
   public Object[] toArray()
   {
      throw new UnsupportedOperationException();
   }

   /** {@inheritDoc} */
   @Override
   public <E> E[] toArray(E[] a)
   {
      throw new UnsupportedOperationException();
   }

   /** {@inheritDoc} */
   @Override
   public boolean containsAll(Collection<?> c)
   {
      throw new UnsupportedOperationException();
   }

   /** {@inheritDoc} */
   @Override
   public boolean addAll(Collection<? extends T> c)
   {
      throw new UnsupportedOperationException();
   }

   /** {@inheritDoc} */
   @Override
   public boolean addAll(int index, Collection<? extends T> c)
   {
      throw new UnsupportedOperationException();
   }

   /** {@inheritDoc} */
   @Override
   public boolean removeAll(Collection<?> c)
   {
      /**
       * FastList和ArrayList的remove方法区别：
         遍历顺序不同。ArrayList从前向后，FastList从后向前。

         为什么要从后向前遍历呢？
         首先，我们希望最后一个元素匹配，因为如果最后一个元素直接匹配numMoved就是0，这样就不用做数组拷贝移位了。作者这样写的目的，一定是因为对于HikariCP，最后一个元素被移除的情况更多。
         为什么对于HikariCP往往是后添加的元素优先被移除呢？
         注意到borrow方法从threadList里尝试获取空闲Entry的时候，正是从FastList的最后一个元素开始remove。
         为什么要从最后一个开始remove呢？
         注意到requite归还连接方法作为唯一一个放入threadList元素的入口，他放入的元素都是未使用的。如果borrow方法配合着从最后一个元素开始尝试获取，那CAS修改状态的成功率是非常高的，因为之前归还的元素可能已经被别的线程从shareList窃取了！

         元素比较方式不同。ArrayList根据入参是否为空，分别使用==或equals判断；FastList只会使用==判断。这样基于内存地址直接比较，效率更高。
       */
      throw new UnsupportedOperationException();
   }

   /** {@inheritDoc} */
   @Override
   public boolean retainAll(Collection<?> c)
   {
      throw new UnsupportedOperationException();
   }

   /** {@inheritDoc} */
   @Override
   public void add(int index, T element)
   {
      throw new UnsupportedOperationException();
   }

   /** {@inheritDoc} */
   @Override
   public int indexOf(Object o)
   {
      throw new UnsupportedOperationException();
   }

   /** {@inheritDoc} */
   @Override
   public int lastIndexOf(Object o)
   {
      throw new UnsupportedOperationException();
   }

   /** {@inheritDoc} */
   @Override
   public ListIterator<T> listIterator()
   {
      throw new UnsupportedOperationException();
   }

   /** {@inheritDoc} */
   @Override
   public ListIterator<T> listIterator(int index)
   {
      throw new UnsupportedOperationException();
   }

   /** {@inheritDoc} */
   @Override
   public List<T> subList(int fromIndex, int toIndex)
   {
      throw new UnsupportedOperationException();
   }

   /** {@inheritDoc} */
   @Override
   public Object clone()
   {
      throw new UnsupportedOperationException();
   }

   /** {@inheritDoc} */
   @Override
   public void forEach(Consumer<? super T> action)
   {
      throw new UnsupportedOperationException();
   }

   /** {@inheritDoc} */
   @Override
   public Spliterator<T> spliterator()
   {
      throw new UnsupportedOperationException();
   }

   /** {@inheritDoc} */
   @Override
   public boolean removeIf(Predicate<? super T> filter)
   {
      throw new UnsupportedOperationException();
   }

   /** {@inheritDoc} */
   @Override
   public void replaceAll(UnaryOperator<T> operator)
   {
      throw new UnsupportedOperationException();
   }

   /** {@inheritDoc} */
   @Override
   public void sort(Comparator<? super T> c)
   {
      throw new UnsupportedOperationException();
   }
}
