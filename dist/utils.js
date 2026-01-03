"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.debounce = (callback, timeout = 0) => {
    let timeoutObj = null;
    return {
        run: (...args) => {
            const context = this;
            clearTimeout(timeoutObj);
            timeoutObj = setTimeout(() => callback.apply(context, args), timeout);
        },
        cancel: () => {
            clearTimeout(timeoutObj);
        },
    };
};
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoidXRpbHMuanMiLCJzb3VyY2VSb290IjoiIiwic291cmNlcyI6WyIuLi9zcmMvdXRpbHMudHMiXSwibmFtZXMiOltdLCJtYXBwaW5ncyI6Ijs7QUFBYSxRQUFBLFFBQVEsR0FBRyxDQUFDLFFBQVEsRUFBRSxPQUFPLEdBQUcsQ0FBQyxFQUFFLEVBQUU7SUFDOUMsSUFBSSxVQUFVLEdBQUcsSUFBSSxDQUFDO0lBRXRCLE9BQU87UUFDSCxHQUFHLEVBQUUsQ0FBQyxHQUFHLElBQVMsRUFBRSxFQUFFO1lBQ2xCLE1BQU0sT0FBTyxHQUFHLElBQUksQ0FBQztZQUNyQixZQUFZLENBQUMsVUFBVSxDQUFDLENBQUM7WUFDekIsVUFBVSxHQUFHLFVBQVUsQ0FDbkIsR0FBRyxFQUFFLENBQUMsUUFBUSxDQUFDLEtBQUssQ0FBQyxPQUFPLEVBQUUsSUFBSSxDQUFDLEVBQ25DLE9BQU8sQ0FDVixDQUFDO1FBQ04sQ0FBQztRQUVELE1BQU0sRUFBRSxHQUFHLEVBQUU7WUFDVCxZQUFZLENBQUMsVUFBVSxDQUFDLENBQUM7UUFDN0IsQ0FBQztLQUNKLENBQUM7QUFDTixDQUFDLENBQUMifQ==