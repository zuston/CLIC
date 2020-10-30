package fdu.daslab.consoletable.enums;

import fdu.daslab.consoletable.table.Cell;
/**
 * @author Du Qinghua
 * @version 1.0
 * @since 2020/10/22 18:14
 */
public enum NullPolicy {

    THROW {
        @Override
        public Cell getCell(Cell cell) {
            throw new IllegalArgumentException("cell or value is null: " + cell);
        }
    },
    NULL_STRING {
        @Override
        public Cell getCell(Cell cell) {
            if(cell == null){
                return new Cell("null");
            }
            cell.setValue("null");
            return cell;
        }
    },
    EMPTY_STRING {
        @Override
        public Cell getCell(Cell cell) {
            if(cell == null){
                return new Cell("");
            }
            cell.setValue("");
            return cell;
        }
    };

    public abstract Cell getCell(Cell cell);

}
